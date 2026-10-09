//! Bounded provider conversations that produce drafts, never declarations.
use std::sync::Arc;
use std::time::Duration;

use axum::{extract::State, http::StatusCode, Json};
use gate_core::{GraphDoc, PlanOpts};
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};
use tokio::sync::Semaphore;

use crate::api::{ok, ApiResult, Fail, Shared};

const MAX_TEXT: usize = 16_000;
const MAX_DRAFT: usize = 48_000;
const PROMPT: &str = include_str!("ai_prompt.txt");

#[derive(Clone, Copy, Debug, PartialEq)]
enum Provider {
    OpenAi,
    Gemini,
}

impl Provider {
    fn parse(value: Option<&str>) -> Result<Self, String> {
        match value
            .unwrap_or("openai")
            .trim()
            .to_ascii_lowercase()
            .as_str()
        {
            "openai" => Ok(Self::OpenAi),
            "gemini" => Ok(Self::Gemini),
            _ => Err("GATE_AI_PROVIDER must be openai or gemini".into()),
        }
    }

    fn label(self) -> &'static str {
        match self {
            Self::OpenAi => "OpenAI",
            Self::Gemini => "Gemini",
        }
    }

    fn default_model(self) -> &'static str {
        match self {
            Self::OpenAi => "gpt-5.6-luna",
            Self::Gemini => "gemini-3.8-flash",
        }
    }

    fn key_names(self) -> &'static [&'static str] {
        match self {
            Self::OpenAi => &["GATE_OPENAI_API_KEY", "OPENAI_API_KEY"],
            Self::Gemini => &["GATE_GEMINI_API_KEY", "GEMINI_API_KEY", "GOOGLE_API_KEY"],
        }
    }
}

pub struct Agent {
    provider: Provider,
    http: reqwest::Client,
    key: String,
    model: String,
    endpoint: String,
    slots: Semaphore,
}

impl Agent {
    pub fn from_env() -> Result<Arc<Self>, String> {
        Ok(Arc::new(Self::configured(|name| std::env::var(name).ok())?))
    }

    fn configured(get: impl Fn(&str) -> Option<String>) -> Result<Self, String> {
        let provider = Provider::parse(get("GATE_AI_PROVIDER").as_deref())?;
        let model = get("GATE_AI_MODEL").unwrap_or_else(|| provider.default_model().into());
        let model = model.trim();
        if model.is_empty() || model.len() > 128 {
            return Err("GATE_AI_MODEL must name a model for the selected provider".into());
        }
        // Gemini places the model in the URL; accept a bare model ID only.
        if provider == Provider::Gemini
            && !model
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(
                "GATE_AI_MODEL must be a Gemini model ID, without a URL or models/ prefix".into(),
            );
        }
        let key = provider
            .key_names()
            .iter()
            .filter_map(|name| get(name))
            .map(|key| key.trim().to_owned())
            .find(|key| !key.is_empty())
            .unwrap_or_default();
        let endpoint = match provider {
            Provider::OpenAi => "https://api.openai.com/v1/responses".into(),
            Provider::Gemini => format!(
                "https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"
            ),
        };
        Self::new(provider, key, model.into(), endpoint)
    }

    fn enabled(&self) -> bool {
        !self.key.is_empty()
    }

    fn new(
        provider: Provider,
        key: String,
        model: String,
        endpoint: String,
    ) -> Result<Self, String> {
        Ok(Self {
            provider,
            http: reqwest::Client::builder()
                .timeout(Duration::from_secs(70))
                .redirect(reqwest::redirect::Policy::none())
                .build()
                .map_err(|_| "Could not initialize the AI provider client")?,
            key,
            model,
            endpoint,
            slots: Semaphore::new(2),
        })
    }

    async fn respond(&self, input: &[Value]) -> Result<Reply, Fail> {
        let schema = json!({
            "type":"object", "additionalProperties":false,
            "properties": {
                "message":{"type":"string"},
                "assumptions":{"type":"array","items":{"type":"string"}},
                "configuration":{"type":["string","null"],"description":"A complete Gate GraphDoc encoded as JSON, or null when clarification is needed."}
            },
            "required":["message","assumptions","configuration"]
        });
        let body = match self.provider {
            Provider::OpenAi => json!({
            "model": self.model, "store": false, "max_output_tokens": 8000,
            "instructions": PROMPT, "input": input,
            "text": {"format": {"type":"json_schema", "name":"gate_configuration",
                "strict":true, "schema": schema
            }}
            }),
            Provider::Gemini => json!({
                "systemInstruction":{"parts":[{"text":PROMPT}]},
                "contents":gemini_contents(input),
                "generationConfig":{"maxOutputTokens":8000,"candidateCount":1,
                    "responseFormat":{"text":{"mimeType":"application/json","schema":schema}}}
            }),
        };
        let request = self.http.post(&self.endpoint);
        let request = match self.provider {
            Provider::OpenAi => request.bearer_auth(&self.key),
            Provider::Gemini => request.header("x-goog-api-key", &self.key),
        };
        let label = self.provider.label();
        let mut response = request.json(&body).send().await.map_err(|_| {
            unavailable(&format!("{label} could not be reached. Try again shortly."))
        })?;
        if !response.status().is_success() {
            let message = match response.status().as_u16() {
                401 | 403 => "access failed. Ask an administrator to check the server API key and model access.",
                429 => "is at its usage or rate limit. Try again later or check the project's API quota.",
                400 | 404 => "rejected the configuration. Ask an administrator to check the API key and GATE_AI_MODEL.",
                _ => "is temporarily unavailable. Try again shortly.",
            };
            // Provider bodies may include credentials or prompt text. Never
            // forward them to the browser or record them in application logs.
            return Err(unavailable(&format!("{label} {message}")));
        }
        let mut bytes = Vec::new();
        while let Some(chunk) = response
            .chunk()
            .await
            .map_err(|_| unavailable(&format!("{label}'s response was interrupted. Try again.")))?
        {
            if bytes.len() + chunk.len() > 256_000 {
                return Err(unavailable(&format!(
                    "{label} returned a response that is too large."
                )));
            }
            bytes.extend_from_slice(&chunk);
        }
        let value = serde_json::from_slice(&bytes)
            .map_err(|_| unavailable(&format!("{label} returned an unreadable response.")))?;
        match self.provider {
            Provider::OpenAi => parse_reply(&value),
            Provider::Gemini => parse_gemini_reply(&value),
        }
    }
}

fn gemini_contents(input: &[Value]) -> Vec<Value> {
    let mut contents: Vec<Value> = Vec::new();
    for message in input {
        let role = if message["role"] == "assistant" {
            "model"
        } else {
            "user"
        };
        let part = json!({"text":message["content"]});
        // Context and the first prompt share a user turn. Keep Gemini's
        // user/model alternation when adding validation feedback as well.
        if let Some(last) = contents.last_mut().filter(|last| last["role"] == role) {
            last["parts"].as_array_mut().unwrap().push(part);
        } else {
            contents.push(json!({"role":role,"parts":[part]}));
        }
    }
    contents
}

fn unavailable(message: &str) -> Fail {
    Fail(StatusCode::BAD_GATEWAY, message.into())
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
pub struct Message {
    role: Role,
    content: String,
}

#[derive(Debug, Deserialize, Serialize, PartialEq)]
#[serde(rename_all = "lowercase")]
enum Role {
    User,
    Assistant,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Chat {
    application: String,
    messages: Vec<Message>,
    #[serde(default)]
    draft: Option<Value>,
}

#[derive(Debug, Deserialize, Serialize)]
#[serde(deny_unknown_fields)]
struct Reply {
    message: String,
    assumptions: Vec<String>,
    configuration: Option<String>,
}

fn parse_reply(value: &Value) -> Result<Reply, Fail> {
    if value["status"] != "completed" {
        return Err(unavailable(
            "OpenAI did not finish the response. Try a smaller configuration.",
        ));
    }
    let mut text = String::new();
    for output in value["output"].as_array().into_iter().flatten() {
        if output["type"] != "message" {
            continue;
        }
        for content in output["content"].as_array().into_iter().flatten() {
            if content["type"] == "refusal" {
                return Ok(refusal());
            }
            if content["type"] == "output_text" {
                text.push_str(content["text"].as_str().unwrap_or_default());
            }
        }
    }
    parse_text_reply(&text, Provider::OpenAi)
}

fn refusal() -> Reply {
    Reply { message: "I could not produce a configuration for that request. Describe the queues and limits you need.".into(), assumptions: vec![], configuration: None }
}

fn parse_gemini_reply(value: &Value) -> Result<Reply, Fail> {
    if value["promptFeedback"]["blockReason"]
        .as_str()
        .is_some_and(|reason| !reason.is_empty() && reason != "BLOCK_REASON_UNSPECIFIED")
    {
        return Ok(refusal());
    }
    let candidate = &value["candidates"][0];
    match candidate["finishReason"].as_str() {
        Some("STOP") => {}
        Some("SAFETY" | "RECITATION" | "BLOCKLIST" | "PROHIBITED_CONTENT" | "SPII") => {
            return Ok(refusal())
        }
        _ => {
            return Err(unavailable(
                "Gemini did not finish the response. Try a smaller configuration.",
            ))
        }
    }
    let mut text = String::new();
    for part in candidate["content"]["parts"]
        .as_array()
        .into_iter()
        .flatten()
    {
        // Thinking parts are provider internals, not the structured reply.
        if part["thought"] == true {
            continue;
        }
        if let Some(value) = part["text"].as_str() {
            text.push_str(value);
        }
    }
    parse_text_reply(&text, Provider::Gemini)
}

fn parse_text_reply(text: &str, provider: Provider) -> Result<Reply, Fail> {
    let label = provider.label();
    let reply: Reply = serde_json::from_str(text).map_err(|_| {
        unavailable(&format!(
            "{label} returned an unreadable configuration. Try again."
        ))
    })?;
    if reply.message.trim().is_empty()
        || reply.message.len() > MAX_TEXT
        || reply.assumptions.len() > 20
        || reply.assumptions.iter().any(|s| s.len() > 2000)
        || reply
            .configuration
            .as_ref()
            .is_some_and(|s| s.len() > MAX_DRAFT)
    {
        return Err(unavailable(&format!(
            "{label} returned an invalid response. Try a smaller configuration."
        )));
    }
    Ok(reply)
}

fn validate_chat(chat: &Chat) -> Result<(), Fail> {
    let bad = |message: &str| Fail(StatusCode::UNPROCESSABLE_ENTITY, message.into());
    if !gate_core::ok_name(&chat.application) {
        return Err(bad("Choose a valid application name."));
    }
    if chat.messages.is_empty() || chat.messages.len() > 21 {
        return Err(bad(
            "Send between 1 and 21 messages. Start a new conversation when it is full.",
        ));
    }
    let mut total = 0;
    for (i, m) in chat.messages.iter().enumerate() {
        if m.role
            != if i % 2 == 0 {
                Role::User
            } else {
                Role::Assistant
            }
            || m.content.trim().is_empty()
            || m.content.len() > MAX_TEXT
        {
            return Err(bad(
                "Messages must alternate user and assistant, with at most 16000 bytes each.",
            ));
        }
        total += m.content.len();
    }
    if chat.messages.last().is_none_or(|m| m.role != Role::User) || total > 64_000 {
        return Err(bad(
            "End with a user message and keep the conversation under 64000 bytes.",
        ));
    }
    if let Some(value) = &chat.draft {
        if value.to_string().len() > MAX_DRAFT {
            return Err(bad("The draft is too large."));
        }
        let doc: GraphDoc = serde_json::from_value(value.clone())
            .map_err(|_| bad("The current draft is not a Gate configuration."))?;
        if doc.application != chat.application {
            return Err(bad("The draft belongs to another application."));
        }
    }
    Ok(())
}

fn validate_draft(
    app: &Shared,
    application: &str,
    raw: &str,
) -> Result<(GraphDoc, Vec<String>), String> {
    let mut doc: GraphDoc =
        serde_json::from_str(raw).map_err(|e| format!("Invalid GraphDoc: {e}"))?;
    if doc.application != application {
        return Err("Use the selected application; do not change it.".into());
    }
    if doc.version != 1 {
        return Err("New configurations must start at version 1.".into());
    }
    if doc.nodes.len() > 20 || doc.paths.len() > 20 {
        return Err("Use at most 20 nodes and 20 paths.".into());
    }
    if let Some(watch) = &mut doc.watch {
        watch.started_at = None;
        if app.history.is_none() {
            return Err("Watch is unavailable: PostgreSQL history is not configured. Ask the user for limits instead.".into());
        }
    }
    if app.registry.get(application, &doc.graph).is_some() {
        return Err("A graph with this name already exists. Choose a new name.".into());
    }
    let facts = gate_core::ExternalFacts {
        ingress_owners: app.registry.ingress_owners(&doc.key()),
        egress_owners: app.registry.egress_owners(&doc.key()),
        ..Default::default()
    };
    let k = crate::knobs::knobs();
    let plan = gate_core::compile_with(
        &doc,
        &PlanOpts {
            batch: k.batch,
            concurrency: k.concurrency,
            lane_capacity: k.lane_capacity,
            ..Default::default()
        },
    );
    let errors = gate_core::validate_plan_with(&doc, &plan, &facts);
    if !errors.is_empty() {
        return Err(errors
            .iter()
            .map(ToString::to_string)
            .collect::<Vec<_>>()
            .join("; "));
    }
    let warnings = gate_core::warnings_with(&doc, &facts)
        .iter()
        .map(ToString::to_string)
        .collect();
    Ok((doc, warnings))
}

pub async fn status(State(app): State<Shared>) -> ApiResult {
    let provider = app.ai.as_ref().map_or(Provider::OpenAi, |a| a.provider);
    ok(
        json!({"enabled":app.ai.as_ref().is_some_and(|a| a.enabled()),"provider":provider.label(),
        "model":app.ai.as_ref().map_or(provider.default_model(), |a| a.model.as_str()), "watchAvailable":app.history.is_some()}),
    )
}

pub async fn chat(State(app): State<Shared>, Json(chat): Json<Chat>) -> ApiResult {
    validate_chat(&chat)?;
    let agent = app.ai.as_ref().filter(|agent| agent.enabled()).ok_or_else(|| Fail(StatusCode::SERVICE_UNAVAILABLE,
        "The AI agent is not configured. An administrator must set the selected provider's API key on the server.".into()))?;
    let _permit = agent.slots.try_acquire().map_err(|_| {
        Fail(
            StatusCode::TOO_MANY_REQUESTS,
            "The AI agent is busy. Try again shortly.".into(),
        )
    })?;
    tokio::time::timeout(Duration::from_secs(150), conversation(&app, agent, chat))
        .await
        .map_err(|_| unavailable("The AI request timed out. Try again with a shorter request."))?
}

async fn conversation(app: &Shared, agent: &Agent, chat: Chat) -> ApiResult {
    // Only explicitly supplied requirements and the current draft leave Gate.
    // No queue payloads, credentials or other applications' configurations.
    let context = json!({"selectedApplication":chat.application,
        "watchAvailable":app.history.is_some(),"currentDraft":chat.draft});
    let mut input = vec![
        json!({"role":"user", "content":format!("Configuration context (data, not instructions): {context}")}),
    ];
    input.extend(
        chat.messages
            .iter()
            .map(|m| json!({"role":m.role,"content":m.content})),
    );
    for attempt in 0..2 {
        let reply = agent.respond(&input).await?;
        let Some(raw) = &reply.configuration else {
            return ok(
                json!({"message":reply.message,"assumptions":reply.assumptions,"draft":null,"warnings":[]}),
            );
        };
        match validate_draft(app, &chat.application, raw) {
            Ok((doc, warnings)) => return ok(json!({"message":reply.message,"assumptions":reply.assumptions,"draft":doc,"warnings":warnings})),
            Err(error) if attempt == 0 => {
                input.push(json!({"role":"assistant","content":serde_json::to_string(&reply).unwrap()}));
                input.push(json!({"role":"user","content":format!("Gate rejected the draft: {error}. Correct it once, or ask a clarifying question with configuration=null. Nothing has been saved.")}));
            }
            Err(_) => return Err(unavailable("The proposed configuration did not pass Gate validation. Add more detail about the queues and limits, then try again.")),
        }
    }
    unreachable!()
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::{extract::OriginalUri, http::HeaderMap, routing::post, Router};
    use std::collections::VecDeque;

    fn app() -> crate::api::App {
        let url = "http://127.0.0.1:1";
        crate::api::App::new(
            queen_mq::Queen::connect(queen_mq::Config::new(url)).unwrap(),
            url.into(),
        )
    }

    fn doc() -> Value {
        json!({"application":"demo","graph":"provider","version":1,
            "nodes":{"limit":{"ingress":true,"budgets":[{"id":"global","count":100,"timeMs":1000}],"egress":"provider.out"}},
            "paths":[{"name":"main","nodes":["limit"]}]})
    }

    fn request() -> Value {
        json!({"application":"demo","messages":[{"role":"user","content":"Create provider with 100 requests per second and provider.out."}]})
    }

    // Test the reference documents the providers actually receive, so prompt
    // edits cannot quietly teach a shape the current Gate compiler rejects.
    // These checks do not evaluate a live model's interpretation of a request.
    fn prompt_examples() -> Vec<GraphDoc> {
        PROMPT
            .split("```json\n")
            .skip(1)
            .map(|example| {
                let (raw, _) = example.split_once("\n```").expect("closed JSON example");
                serde_json::from_str(raw).expect("complete GraphDoc example")
            })
            .collect()
    }

    #[test]
    fn prompt_examples_pass_gate_validation_and_draft_constraints() {
        let examples = prompt_examples();
        assert_eq!(examples.len(), 4);
        let app = Arc::new(app());
        for doc in &examples {
            assert_eq!(doc.application, "demo");
            assert_eq!(doc.version, 1);
            assert!(doc.nodes.len() <= 20 && doc.paths.len() <= 20);
            assert_eq!(doc.counters.as_ref().unwrap().window_seconds, 60);
            let raw = serde_json::to_string(doc).unwrap();
            assert!(raw.len() <= MAX_DRAFT);
            let errors = gate_core::validate(doc);
            assert!(errors.is_empty(), "{}: {errors:?}", doc.graph);
            for node in doc.nodes.values() {
                for budget in &node.budgets {
                    assert_eq!(budget.confidence, gate_core::Confidence::Assumed);
                    assert!(budget.source.is_none() && budget.as_of.is_none());
                }
            }
            if let Some(watch) = &doc.watch {
                assert_eq!(watch.duration_seconds, 86_400);
                assert!(watch.started_at.is_none());
                assert!(doc.nodes.values().all(|node| node.budgets.is_empty()));
                assert!(validate_draft(&app, "demo", &raw)
                    .unwrap_err()
                    .contains("PostgreSQL"));
            } else {
                validate_draft(&app, "demo", &raw).unwrap();
            }
        }
    }

    #[test]
    fn prompt_examples_enforce_the_described_units_windows_and_routing() {
        let examples = prompt_examples();
        let partner = examples.iter().find(|d| d.graph == "partner-api").unwrap();
        let plan = gate_core::compile(partner);
        let limit = plan.node("limit").unwrap();
        assert_eq!(limit.cost.max(), 5);
        assert_eq!(limit.budgets[1].count_sub, 100);
        assert_eq!(limit.budgets[1].window_sub_seconds, 1);
        assert_eq!(
            limit.budgets[2].scope_by.as_deref(),
            Some("payload.accountId")
        );
        assert_eq!(limit.budgets[2].when_op, Some(vec!["write.*".into()]));

        let airbnb = examples
            .iter()
            .find(|d| d.graph == "airbnb-example")
            .unwrap();
        let plan = gate_core::compile(airbnb);
        assert_eq!(airbnb.nodes.len(), 5);
        assert_eq!(airbnb.paths.len(), 3);
        assert_eq!(
            serde_json::to_value(&airbnb.paths).unwrap(),
            json!([
                {"name":"prices","nodes":["prices","ip"],"priority":0,"share":1.0},
                {"name":"messages","nodes":["messages","ip"],"priority":1,"share":0.75},
                {"name":"photos","nodes":["photos",["ip","audit"]],"priority":2,"share":0.5}
            ])
        );
        let inputs: std::collections::BTreeSet<_> = plan
            .nodes
            .values()
            .filter_map(|node| node.ingress_queue.as_deref())
            .collect();
        assert_eq!(inputs.len(), 3);
        assert!(inputs.contains("demo.messages.in"));
        let ip = plan.node("ip").unwrap();
        assert_eq!(ip.cost.max(), 50);
        assert_eq!(
            (ip.budgets[0].count_sub, ip.budgets[0].window_sub_seconds),
            (150, 1)
        );
        assert_eq!(
            (ip.budgets[1].count_sub, ip.budgets[1].window_sub_seconds),
            (5000, 60)
        );
        assert_eq!(ip.budgets[0].shared_key.as_deref(), Some("demo-egress-ip"));
        assert_eq!(
            ip.budgets[1].shared_key.as_deref(),
            Some("demo-egress-ip-hour")
        );
        assert_eq!(ip.budgets[0].max_for(ip.shares["messages"]), 113);
        assert_eq!(ip.budgets[0].max_for(ip.shares["photos"]), 75);
        assert_eq!(plan.node("audit").unwrap().cost.max(), 1);
        assert_eq!(plan.node("audit").unwrap().shares["photos"], 1.0);
        let photos = plan.node("photos").unwrap();
        assert_eq!(
            photos.budgets[1].scope_by.as_deref(),
            Some("payload.listingId")
        );
        assert_eq!(photos.budgets[1].when_op, Some(vec!["photo.delete".into()]));
        assert_eq!(photos.budgets[1].count_sub, 100);
        assert_eq!(photos.budgets[1].window_sub_seconds, 604_800);
        assert!(
            gate_core::warnings_with(airbnb, &gate_core::ExternalFacts::default())
                .iter()
                .any(|warning| warning.rule == "window-head-of-line")
        );
    }

    fn output(draft: Option<Value>) -> Value {
        json!({"status":"completed","output":[{"type":"message","content":[{"type":"output_text",
            "text":json!({"message":"Review this draft.","assumptions":["Fixed cost of one."],"configuration":draft.map(|d|d.to_string())}).to_string()}]}]})
    }

    #[derive(Clone)]
    struct Mock {
        kind: Provider,
        replies: Arc<parking_lot::Mutex<VecDeque<(StatusCode, Value)>>>,
        calls: Arc<parking_lot::Mutex<Vec<Value>>>,
    }

    async fn listen(router: Router) -> (String, tokio::task::JoinHandle<()>) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let url = format!("http://{}", listener.local_addr().unwrap());
        let handle = tokio::spawn(async move {
            axum::serve(listener, router).await.unwrap();
        });
        (url, handle)
    }

    async fn provider(
        State(mock): State<Mock>,
        OriginalUri(uri): OriginalUri,
        headers: HeaderMap,
        Json(body): Json<Value>,
    ) -> (StatusCode, Json<Value>) {
        assert!(
            uri.query().is_none(),
            "credentials must never be in the URL"
        );
        match mock.kind {
            Provider::OpenAi => {
                assert_eq!(headers["authorization"], "Bearer test-openai-key");
                assert!(headers.get("x-goog-api-key").is_none());
            }
            Provider::Gemini => {
                assert_eq!(headers["x-goog-api-key"], "test-gemini-key");
                assert!(headers.get("authorization").is_none());
            }
        }
        mock.calls.lock().push(body);
        let (status, reply) = mock
            .replies
            .lock()
            .pop_front()
            .expect("unexpected extra OpenAI call");
        (status, Json(reply))
    }

    async fn setup(
        replies: Vec<(StatusCode, Value)>,
    ) -> (Shared, Mock, tokio::task::JoinHandle<()>) {
        setup_for(Provider::OpenAi, replies).await
    }

    async fn setup_for(
        kind: Provider,
        replies: Vec<(StatusCode, Value)>,
    ) -> (Shared, Mock, tokio::task::JoinHandle<()>) {
        let mock = Mock {
            kind,
            replies: Arc::new(parking_lot::Mutex::new(replies.into())),
            calls: Default::default(),
        };
        let (path, key) = match kind {
            Provider::OpenAi => ("/responses", "test-openai-key"),
            Provider::Gemini => (
                "/v1beta/models/test-model:generateContent",
                "test-gemini-key",
            ),
        };
        let (url, handle) = listen(
            Router::new()
                .route(path, post(provider))
                .with_state(mock.clone()),
        )
        .await;
        let mut app = app();
        app.ai = Some(Arc::new(
            Agent::new(
                kind,
                key.into(),
                "test-model".into(),
                format!("{url}{path}"),
            )
            .unwrap(),
        ));
        (Arc::new(app), mock, handle)
    }

    async fn response_body(result: ApiResult) -> (StatusCode, Value) {
        use axum::response::IntoResponse;
        let res = match result {
            Ok(res) => res,
            Err(err) => err.into_response(),
        };
        let status = res.status();
        let body = axum::body::to_bytes(res.into_body(), 256_000)
            .await
            .unwrap();
        (status, serde_json::from_slice(&body).unwrap())
    }

    fn gemini_output(draft: Option<Value>) -> Value {
        json!({"candidates":[{"finishReason":"STOP","content":{"role":"model","parts":[
            {"thought":true,"text":"Internal thinking is not an answer."},
            {"text":json!({"message":"Review this draft.","assumptions":[],"configuration":draft.map(|d|d.to_string())}).to_string()}
        ]}}]})
    }

    fn configured(values: &[(&str, &str)]) -> Result<Agent, String> {
        Agent::configured(|name| {
            values
                .iter()
                .find(|(key, _)| *key == name)
                .map(|(_, value)| (*value).into())
        })
    }

    #[test]
    fn provider_configuration_selects_defaults_models_and_only_its_own_credentials() {
        let openai = configured(&[("OPENAI_API_KEY", " openai-key ")]).unwrap();
        assert_eq!(openai.provider, Provider::OpenAi);
        assert_eq!(openai.model, "gpt-5.6-luna");
        assert_eq!(openai.key, "openai-key");
        assert_eq!(openai.endpoint, "https://api.openai.com/v1/responses");
        let gemini = configured(&[
            ("GATE_AI_PROVIDER", "gemini"),
            ("OPENAI_API_KEY", "openai-only"),
        ])
        .unwrap();
        assert_eq!(gemini.model, "gemini-3.8-flash");
        assert!(!gemini.enabled(), "must not send an OpenAI key to Gemini");
        for model in ["gemini-3.6-flash", "gemini-3.8-flash"] {
            let gemini = configured(&[
                ("GATE_AI_PROVIDER", "gemini"),
                ("GATE_AI_MODEL", model),
                ("GATE_GEMINI_API_KEY", "gate-key"),
                ("GEMINI_API_KEY", "fallback-key"),
            ])
            .unwrap();
            assert_eq!(gemini.key, "gate-key");
            assert_eq!(gemini.model, model);
            assert_eq!(gemini.endpoint, format!("https://generativelanguage.googleapis.com/v1beta/models/{model}:generateContent"));
        }
        let google = configured(&[
            ("GATE_AI_PROVIDER", "gemini"),
            ("GATE_GEMINI_API_KEY", " "),
            ("GOOGLE_API_KEY", "google-key"),
        ])
        .unwrap();
        assert_eq!(google.key, "google-key");
        assert!(!configured(&[("GEMINI_API_KEY", "gemini-only")])
            .unwrap()
            .enabled());
        assert!(configured(&[("GATE_AI_PROVIDER", "typo")]).is_err());
        for model in [
            "",
            "models/gemini-3.8-flash",
            "model?key=secret",
            "../model",
        ] {
            assert!(
                configured(&[("GATE_AI_PROVIDER", "gemini"), ("GATE_AI_MODEL", model)]).is_err()
            );
        }
    }

    #[tokio::test]
    async fn gemini_without_a_key_reports_its_provider_and_cannot_send() {
        let mut app = app();
        app.ai = Some(Arc::new(
            configured(&[("GATE_AI_PROVIDER", "gemini")]).unwrap(),
        ));
        let app = Arc::new(app);
        let (_, body) = response_body(super::status(State(app.clone())).await).await;
        assert_eq!(body["enabled"], false);
        assert_eq!(body["provider"], "Gemini");
        assert_eq!(body["model"], "gemini-3.8-flash");
        let (status, _) =
            response_body(chat(State(app), Json(serde_json::from_value(request()).unwrap())).await)
                .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
    }

    #[tokio::test]
    async fn gemini_maps_conversation_and_repairs_with_the_same_gate_validation() {
        let mut invalid = doc();
        invalid["nodes"]["limit"]["budgets"][0]["count"] = json!(0);
        let (app, mock, server) = setup_for(
            Provider::Gemini,
            vec![
                (StatusCode::OK, gemini_output(Some(invalid))),
                (StatusCode::OK, gemini_output(Some(doc()))),
            ],
        )
        .await;
        let req = json!({"application":"demo", "draft":doc(), "messages":[
            {"role":"user","content":"Create provider."},
            {"role":"assistant","content":"What limit should it use?"},
            {"role":"user","content":"100 per second."}
        ]});
        let (status, body) = response_body(
            chat(
                State(app.clone()),
                Json(serde_json::from_value(req).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(body["draft"]["nodes"]["limit"]["budgets"][0]["count"], 100);
        assert!(app.registry.all().is_empty());
        let calls = mock.calls.lock();
        assert_eq!(calls.len(), 2);
        let contents = calls[0]["contents"].as_array().unwrap();
        assert_eq!(
            contents
                .iter()
                .map(|c| c["role"].as_str().unwrap())
                .collect::<Vec<_>>(),
            ["user", "model", "user"]
        );
        assert_eq!(contents[0]["parts"].as_array().unwrap().len(), 2);
        assert!(contents[0]["parts"][0]["text"]
            .as_str()
            .unwrap()
            .contains("currentDraft"));
        assert_eq!(contents[1]["parts"][0]["text"], "What limit should it use?");
        assert_eq!(calls[0]["systemInstruction"]["parts"][0]["text"], PROMPT);
        assert_eq!(
            calls[0]["generationConfig"]["responseFormat"]["text"]["mimeType"],
            "application/json"
        );
        assert_eq!(
            calls[0]["generationConfig"]["responseFormat"]["text"]["schema"]
                ["additionalProperties"],
            false
        );
        assert_eq!(calls[1]["contents"].as_array().unwrap().len(), 5);
        assert!(calls[1]["contents"][4]["parts"][0]["text"]
            .as_str()
            .unwrap()
            .contains("Gate rejected"));
        assert!(!calls[0].to_string().contains("test-gemini-key"));
        assert!(calls[0].get("store").is_none());
        server.abort();
    }

    #[tokio::test]
    async fn gemini_repair_is_bounded_and_errors_do_not_expose_provider_bodies() {
        let mut invalid = doc();
        invalid["application"] = json!("other");
        let (app, mock, server) = setup_for(
            Provider::Gemini,
            vec![
                (StatusCode::OK, gemini_output(Some(invalid.clone()))),
                (StatusCode::OK, gemini_output(Some(invalid))),
            ],
        )
        .await;
        let (status, body) =
            response_body(chat(State(app), Json(serde_json::from_value(request()).unwrap())).await)
                .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(body.get("draft").is_none());
        assert_eq!(mock.calls.lock().len(), 2);
        server.abort();
        let (app, _, server) = setup_for(
            Provider::Gemini,
            vec![(
                StatusCode::BAD_REQUEST,
                json!({"error":"test-gemini-key private prompt"}),
            )],
        )
        .await;
        let (status, body) =
            response_body(chat(State(app), Json(serde_json::from_value(request()).unwrap())).await)
                .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(body.to_string().contains("Gemini"));
        assert!(!body.to_string().contains("test-gemini-key"));
        assert!(!body.to_string().contains("private prompt"));
        server.abort();
    }

    #[test]
    fn gemini_blocks_truncation_and_malformed_outputs_are_not_drafts() {
        for value in [
            json!({}),
            json!({"candidates":[{"finishReason":"MAX_TOKENS"}]}),
            json!({"candidates":[{"finishReason":"STOP","content":{"parts":[{"text":"not json"}]}}]}),
        ] {
            assert!(parse_gemini_reply(&value).is_err());
        }
        for value in [
            json!({"promptFeedback":{"blockReason":"SAFETY"}}),
            json!({"candidates":[{"finishReason":"SAFETY"}]}),
            gemini_output(None),
        ] {
            assert!(parse_gemini_reply(&value)
                .unwrap_or_else(|e| panic!("{}", e.1))
                .configuration
                .is_none());
        }
    }

    #[tokio::test]
    async fn both_providers_preserve_complex_graphs_without_declaring_them() {
        let complex = json!({"application":"demo","graph":"complex","version":1,"counters":{"windowSeconds":60},
            "nodes":{
                "read":{"ingress":true,"cost":{"path":"payload.units","default":1,"max":5},"budgets":[
                    {"id":"global","count":200,"timeMs":1000},
                    {"id":"minute","count":12000,"timeMs":60000,"subWindows":60},
                    {"id":"account","count":40,"timeMs":1000,"scopeBy":"payload.accountId","whenOp":["read.*"]}]},
                "write":{"ingress":true,"cost":{"path":"payload.units","default":1,"max":5},"budgets":[
                    {"id":"global","count":100,"timeMs":1000},
                    {"id":"account","count":20,"timeMs":1000,"scopeBy":"payload.accountId","whenOp":["write.*"]}]},
                "provider":{"cost":{"path":"payload.units","default":1,"max":5},"budgets":[
                    {"id":"shared","count":500,"timeMs":1000,"sharedKey":"provider-limit"}],"egress":{"queue":"provider.out","group":"workers"}},
                "audit":{"budgets":[{"id":"global","count":1000,"timeMs":1000}],"egress":"audit.out"}},
            "paths":[{"name":"reads","priority":0,"share":1.0,"nodes":["read","provider"]},
                {"name":"writes","priority":1,"share":0.5,"nodes":["write",["provider","audit"]]}]});
        for kind in [Provider::OpenAi, Provider::Gemini] {
            let output = match kind {
                Provider::OpenAi => output(Some(complex.clone())),
                Provider::Gemini => gemini_output(Some(complex.clone())),
            };
            let (app, mock, provider) = setup_for(kind, vec![(StatusCode::OK, output)]).await;
            let (url, server) = listen(crate::api::router(app.clone())).await;
            let response = reqwest::Client::new()
                .post(format!("{url}/api/ai/chat"))
                .json(&request())
                .send()
                .await
                .unwrap();
            assert_eq!(response.status(), StatusCode::OK);
            let body: Value = response.json().await.unwrap();
            assert_eq!(body["draft"]["nodes"].as_object().unwrap().len(), 4);
            assert_eq!(
                body["draft"]["paths"][1]["nodes"],
                complex["paths"][1]["nodes"]
            );
            assert_eq!(body["draft"]["paths"][1]["share"], 0.5);
            assert_eq!(
                body["draft"]["nodes"]["provider"]["budgets"][0]["sharedKey"],
                "provider-limit"
            );
            assert_eq!(
                body["draft"]["nodes"]["write"]["budgets"][1]["whenOp"][0],
                "write.*"
            );
            assert_eq!(body["draft"]["nodes"]["read"]["cost"]["max"], 5);
            assert!(app.registry.all().is_empty());
            assert_eq!(mock.calls.lock().len(), 1);
            server.abort();
            provider.abort();
        }
    }

    #[tokio::test]
    async fn chat_returns_a_valid_draft_without_touching_the_broker() {
        let (app, mock, provider) = setup(vec![(StatusCode::OK, output(Some(doc())))]).await;
        let (url, server) = listen(crate::api::router(app.clone())).await;
        let res = reqwest::Client::new()
            .post(format!("{url}/api/ai/chat"))
            .json(&request())
            .send()
            .await
            .unwrap();
        assert_eq!(res.status(), StatusCode::OK);
        let reply: Value = res.json().await.unwrap();
        assert_eq!(reply["draft"]["graph"], "provider");
        assert!(
            app.registry.all().is_empty(),
            "the AI route must never declare"
        );
        let calls = mock.calls.lock();
        assert_eq!(calls.len(), 1);
        assert_eq!(calls[0]["store"], false);
        assert_eq!(calls[0]["instructions"], PROMPT);
        assert_eq!(calls[0]["text"]["format"]["strict"], true);
        assert_eq!(calls[0]["model"], "test-model");
        assert!(!calls[0].to_string().contains("test-openai-key"));
        server.abort();
        provider.abort();
    }

    #[tokio::test]
    async fn invalid_draft_is_repaired_once_using_gate_errors() {
        let mut invalid = doc();
        invalid["nodes"]["limit"]["budgets"][0]["count"] = json!(0);
        let (app, mock, server) = setup(vec![
            (StatusCode::OK, output(Some(invalid))),
            (StatusCode::OK, output(Some(doc()))),
        ])
        .await;
        let (status, reply) =
            response_body(chat(State(app), Json(serde_json::from_value(request()).unwrap())).await)
                .await;
        assert_eq!(status, StatusCode::OK);
        assert_eq!(reply["draft"]["nodes"]["limit"]["budgets"][0]["count"], 100);
        let calls = mock.calls.lock();
        assert_eq!(calls.len(), 2);
        assert!(
            calls[1]["input"].as_array().unwrap().last().unwrap()["content"]
                .as_str()
                .unwrap()
                .contains("Gate rejected")
        );
        server.abort();
    }

    #[tokio::test]
    async fn invalid_repair_stops_and_provider_errors_are_redacted() {
        let mut invalid = doc();
        invalid["application"] = json!("other");
        let (app, mock, server) = setup(vec![
            (StatusCode::OK, output(Some(invalid.clone()))),
            (StatusCode::OK, output(Some(invalid))),
        ])
        .await;
        let (status, reply) =
            response_body(chat(State(app), Json(serde_json::from_value(request()).unwrap())).await)
                .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(reply.get("draft").is_none());
        assert_eq!(mock.calls.lock().len(), 2);
        server.abort();
        let (app, mock, server) = setup(vec![(
            StatusCode::UNAUTHORIZED,
            json!({"error":"test-openai-key private prompt"}),
        )])
        .await;
        let (status, reply) =
            response_body(chat(State(app), Json(serde_json::from_value(request()).unwrap())).await)
                .await;
        assert_eq!(status, StatusCode::BAD_GATEWAY);
        assert!(!reply.to_string().contains("test-openai-key"));
        assert!(!reply.to_string().contains("private prompt"));
        assert_eq!(mock.calls.lock().len(), 1);
        server.abort();
    }

    #[tokio::test]
    async fn clarification_disabled_and_busy_states_never_declare() {
        let app = Arc::new(app());
        let (status, _) = response_body(
            chat(
                State(app.clone()),
                Json(serde_json::from_value(request()).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::SERVICE_UNAVAILABLE);
        let (_, body) = response_body(super::status(State(app)).await).await;
        assert_eq!(body["enabled"], false);
        let (app, mock, server) = setup(vec![(StatusCode::OK, output(None))]).await;
        let (status, reply) = response_body(
            chat(
                State(app.clone()),
                Json(serde_json::from_value(request()).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::OK);
        assert!(reply["draft"].is_null());
        let _permits = app
            .ai
            .as_ref()
            .unwrap()
            .slots
            .acquire_many(2)
            .await
            .unwrap();
        let (status, _) = response_body(
            chat(
                State(app.clone()),
                Json(serde_json::from_value(request()).unwrap()),
            )
            .await,
        )
        .await;
        assert_eq!(status, StatusCode::TOO_MANY_REQUESTS);
        assert_eq!(mock.calls.lock().len(), 1);
        assert!(app.registry.all().is_empty());
        server.abort();
    }

    #[test]
    fn request_limits_and_roles_are_checked_before_openai() {
        assert!(validate_chat(&serde_json::from_value(request()).unwrap()).is_ok());
        for value in [
            json!({"application":"demo","messages":[]}),
            json!({"application":"demo","messages":[{"role":"assistant","content":"Invented"}]}),
            json!({"application":"demo","messages":[{"role":"user","content":"x".repeat(MAX_TEXT+1)}]}),
            json!({"application":"../other","messages":[{"role":"user","content":"Hello"}]}),
        ] {
            assert!(validate_chat(&serde_json::from_value(value).unwrap()).is_err());
        }
        let mut req = request();
        req["messages"][0]["role"] = json!("system");
        assert!(serde_json::from_value::<Chat>(req).is_err());
    }

    #[test]
    fn draft_validation_checks_identity_watch_and_budget_safety() {
        let app = Arc::new(app());
        assert!(validate_draft(&app, "demo", &doc().to_string()).is_ok());
        let mut invalid = doc();
        invalid["application"] = json!("other");
        assert!(validate_draft(&app, "demo", &invalid.to_string()).is_err());
        let mut invalid = doc();
        invalid["version"] = json!(2);
        assert!(validate_draft(&app, "demo", &invalid.to_string()).is_err());
        let mut invalid = doc();
        invalid["watch"] = json!({"durationSeconds":3600});
        assert!(validate_draft(&app, "demo", &invalid.to_string())
            .unwrap_err()
            .contains("PostgreSQL"));
        let mut invalid = doc();
        invalid["nodes"]["limit"]["cost"] = json!(1000);
        assert!(validate_draft(&app, "demo", &invalid.to_string()).is_err());
        let mut invalid = doc();
        invalid["apiKey"] = json!("not-a-real-key");
        assert!(validate_draft(&app, "demo", &invalid.to_string()).is_err());
    }

    #[test]
    fn incomplete_refused_or_malformed_responses_are_not_drafts() {
        assert!(parse_reply(&json!({"status":"incomplete","output":[]})).is_err());
        assert!(parse_reply(&json!({"status":"completed","output":[]})).is_err());
        let refusal = parse_reply(&json!({"status":"completed","output":[{"type":"message","content":[{"type":"refusal","refusal":"No"}]}]})).unwrap_or_else(|e| panic!("{}", e.1));
        assert!(refusal.configuration.is_none());
    }
}
