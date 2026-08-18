# La spec del target

Rev 0.1 del 2026-08-17. **Bozza aperta**: le decisioni non chiuse sono in §12 e vanno chiuse prima del primo `PUT` da un chiamante reale, perché dopo la forma non si rinomina, si migra.

Il documento che un chiamante manda a `PUT /v1/targets/{name}`. Dichiara **cosa ci limita** e **come vogliamo essere ritmati**; `queen-rrl` ne deriva code, partizioni, stream e consumer. Il razionale della forma sta in `PLAN_RRL.md` §15; qui c'è solo il contratto.

**Cosa non è.** Non è una descrizione dell'API remota: non ci sono URL, header, autenticazione, retry. `queen-rrl` non fa mai la chiamata e non deve saper parlare con nessuno. Un target è un **secchiello con dei nomi sopra**.

---

## 1. Il documento in una schermata

Airbnb, che è il caso peggiore: quattro finestre annidate a livello IP, un tetto per endpoint, un cap per host, un cap settimanale minuscolo per entità, e una mutua esclusione per listing.

```yaml
name: airbnb
version: 3

egress: nat-pod-default          # §3.5 — budget condiviso fra target

budgets:
  - id: ip-10s
    cap: 2000
    period: 10s
    alignment: rolling
    scope: []                     # un solo contatore per tutto il target
    confidence: documented
    source: "developer.withairbnb.com/homes/docs/rate-limits"
    asOf: 2026-05-19

  - id: ip-5m   { cap: 20000,   period: 5m, alignment: rolling, scope: [], confidence: documented }
  - id: ip-1h   { cap: 200000,  period: 1h, alignment: rolling, scope: [], confidence: documented }
  - id: ip-1d   { cap: 4500000, period: 1d, alignment: rolling, scope: [], confidence: documented }

  - id: messaging-post
    cap: 100
    period: 1m
    alignment: calendar
    match: { op: ["messaging.send"] }
    scope: []
    confidence: documented

  - id: listings-create
    cap: 100
    period: 1h
    alignment: rolling
    match: { op: ["listing.create"] }
    scope: [host]                 # un contatore per host
    maxKeys: 5000
    confidence: documented

  - id: photo-delete-weekly
    cap: 100
    period: 7d
    alignment: rolling
    match: { op: ["photo.delete"] }
    scope: [entity]
    maxKeys: 200000
    store: kv                     # §3.4 — alta cardinalità, non sta nello stato del gate
    confidence: documented

lanes:
  - { name: urgent, cap: ceiling,                concurrency: 8 }
  - { name: bulk,   cap: ceiling-minus-measured, concurrency: 24, floor: 0.2, default: true }

cost:
  field: httpCost
  default: 1
  max: 400                        # validato contro ogni cap: §9

pacing:
  leaseSeconds: 1
  batch: 200

admitted:
  partitionBy: entity
  partitions: 512

exclusive:
  - { match: { op: ["pricing.settings", "listing.rooms", "listing.photos"] }, by: entity }

breach:
  - { when: { status: 429 }, outcome: throttled }
  - { when: { status: 200, jsonPath: "$.errors[*].extensions.code", equals: "RATE_LIMIT_EXCEEDED" }, outcome: throttled }
  - { when: { status: 429, jsonPath: "$.error_class", equals: "ConcurrencyLimitError" }, outcome: conflict }
```

---

## 2. Campi di primo livello

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `name` | `string` | sì | — | `^[a-z0-9][a-z0-9-]{0,62}$`. È l'identità: rinominarlo è un target nuovo, non una modifica |
| `version` | `int > 0` | sì | — | va incrementata a ogni cambiamento di classe *migrazione* (§10). Un `PUT` che cambia un campo di quella classe senza incrementarla è **rifiutato** |
| `egress` | `string` | no | `null` | etichetta dell'identità di egresso, informativa: il budget condiviso si referenzia in `budgets[]` (§3.5) |
| `budgets` | `Budget[]` | sì, ≥ 1 | — | §3 |
| `lanes` | `Lane[]` | sì, ≥ 1 | — | §4 |
| `cost` | `Cost` | sì | — | §5. Obbligatorio anche quando è banale, perché il default silenzioso è dove nasce il blocco permanente di §9 |
| `pacing` | `Pacing` | no | `{leaseSeconds: 1, batch: 200}` | §6 |
| `admitted` | `Admitted` | no | `{partitionBy: connection, partitions: 64}` | §7 |
| `exclusive` | `Exclusive[]` | no | `[]` | §8 |
| `breach` | `Breach[]` | no | `[]` | §8.2 |
| `observability` | `Observability` | no | vedi §8.3 | §8.3 |

**Ambiente.** Sandbox e produzione sono **target distinti** (`airbnb`, `airbnb-sandbox`), non una dimensione. Non condividono budget e non devono: la sandbox di Expedia è 5× più stretta della produzione, e un `env` dentro lo stesso target inviterebbe a interpolare fra due numeri che non hanno relazione.

---

## 3. `Budget`

L'unità di limite. Un item di lavoro consuma **tutti** i budget il cui `match` lo seleziona, e viene ammesso solo se **tutti** ammettono. Al diniego la risposta nomina il budget colpevole.

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `id` | `string` | sì | — | univoco nel target. Compare nei dinieghi, nelle metriche e negli allarmi: sceglierlo leggibile |
| `cap` | `int > 0` | sì | — | in **unità di costo**, non in messaggi (§5) |
| `period` | `duration` | sì | — | `10s`, `5m`, `1h`, `7d`. Minimo `1s` |
| `alignment` | `rolling \| calendar` | **sì** | — | nessun default, di proposito: §3.1 |
| `match` | `Match` | no | tutto | §3.2 |
| `scope` | `string[]` | no | `[]` | §3.3 |
| `maxKeys` | `int > 0` | sì se `scope` non è vuoto | — | §3.4 |
| `store` | `gate \| kv` | no | `gate` | §3.4 |
| `confidence` | `documented \| inferred \| assumed` | sì | — | §3.6 |
| `source` | `string` | sì se `documented` | — | citazione o URL |
| `asOf` | `date` | sì se `documented` | — | i numeri invecchiano |

### 3.1 `alignment` non ha default

`rolling` significa "mai più di *cap* in una qualunque finestra di *period*". `calendar` significa "il contatore si azzera al confine dell'orologio". Sono diversi di un fattore 2 al confine: con `calendar` e cap 100/min puoi legittimamente fare 100 chiamate a `12:00:59` e altre 100 a `12:01:00`. Se il vendor intende `rolling` e noi implementiamo `calendar`, quel burst è uno sforo — e sarà la prima cosa che succede sotto carico, non l'ultima.

Nessun default significa che chi dichiara deve **guardare** cosa dice il vendor, e quando il vendor non lo dice deve scrivere `rolling` con `confidence: assumed`, che è la scelta prudente perché è quella più stretta.

### 3.2 `Match` — la selezione, che non può essere sull'URL

```
match:
  op: ["messaging.send", "listing.*"]     # glob sul segmento finale
```

Il gate decide **prima** che la chiamata HTTP esista: non c'è un URL su cui fare match, e non ci sarà mai. La selezione avviene su un `op` — un nome di operazione **dichiarato dal produttore** sull'item di lavoro. Un budget senza `match` seleziona tutto, ed è come si esprime il tetto globale.

`op` è una stringa a segmenti separati da punto, e il glob vale sul suffisso: `listing.*` prende `listing.create` e `listing.rooms`. Nessun regex: un linguaggio di pattern in un file di configurazione è una superficie di guasto che non paghiamo.

**Conseguenza sul chiamante:** l'insieme degli `op` è parte del contratto quanto i budget. Un `op` non dichiarato che arriva al push è un errore, non un default — altrimenti un refuso fa passare il traffico sotto il solo tetto globale, cioè con il limite sbagliato e senza che nessuno se ne accorga.

### 3.3 `scope` — la chiave del contatore

Le dimensioni che compongono la chiave. `[]` è un contatore solo per tutto il target; `[host]` è un contatore per host; `[entity]` uno per entità.

Dimensioni ammesse: `host`, `entity`, `account`, `connection`, `tenant`. Ognuna deve essere presente come attributo sull'item di lavoro quando un budget la usa, altrimenti il push è rifiutato — di nuovo, meglio un errore al push che un contatore sbagliato in silenzio.

`scope` risolve C5 del documento di discovery: *per app*, *per host*, *per listing*, *per machine account* sono tutti la stessa cosa con una dimensione diversa.

### 3.4 `maxKeys` e `store` — dove il budget vive davvero

Un budget con `scope: []` è un numero. Un budget con `scope: [entity]` su un portale con 200.000 listing è **200.000 numeri**, e devono stare da qualche parte.

- `store: gate` (default) — i contatori vivono nel documento di stato della partizione. Lo stato di streams **non ha tetti applicati** e viene riletto per intero a ogni ciclo: un documento grosso è una rilettura grossa, ogni volta. Va bene fino a cardinalità basse.
- `store: kv` — un contatore per riga su `queen.kv`, con `incr` e `max`, dove `applied` è la decisione. È ciò per cui kv è stato scritto, la scadenza è obbligatoria e la potatura la fa lo sweeper.

`maxKeys` è **obbligatorio** su ogni budget con scope, ed è una dichiarazione verificabile, non una stima: il `PUT` è rifiutato se `store: gate` e `maxKeys` supera la soglia di cella. La regola è semplice e va nella prima riga della documentazione del campo: **la cardinalità bassa sta nel gate, l'alta sta in kv.**

Costo dichiarato di `store: kv`: la decisione esce dal ciclo e diventa una chiamata sincrona fuori banda, quindi la `GateFn` non è più pura per quel budget e la spesa non viene annullata se il ciclo aborta. È un prezzo che si paga solo dove serve.

### 3.5 Budget condivisi fra target

Alcuni tetti non sono del portale ma dell'**identità di rete**: due target che escono dallo stesso NAT si contendono lo stesso budget per IP.

> **Un budget che attraversa le partizioni non è applicabile dal gate.** L'isolamento del gate è esattamente coestensivo con la partizione — lo stato è per `(query_id, partition_id, key)`, due target sono due query — quindi nessuna `GateFn` può vedere il contatore dell'altra. Non è una lacuna dell'API: è una proprietà del modello, e le implementazioni possibili sono due.

**La risorsa.** Un budget condiviso non appartiene a nessun target: si dichiara una volta con `PUT /v1/budgets/{id}` e i target lo **referenziano**, così aggiungere un membro non richiede di modificare il budget.

```yaml
# PUT /v1/budgets/nat-pod-default-ip
id: nat-pod-default-ip
cap: 2000
period: 10s
alignment: calendar               # §3.5.3
enforcement: reserve              # reserve | feedback
confidence: documented
source: "developer.withairbnb.com/homes/docs/rate-limits"
asOf: 2026-05-19
```

```yaml
# dentro il target, come una voce di budgets[]
budgets:
  - { shared: nat-pod-default-ip }
```

`store` non è dichiarabile su un budget condiviso: è `kv` per costruzione, perché è l'unico spazio che due partizioni raggiungono entrambe.

#### 3.5.1 `enforcement: reserve` — esatto, con I/O nel percorso

All'inizio di ogni ciclo `queen-rrl` fa **un** `incr` con `delta` = somma dei costi del batch, `max` = `cap`, `required: true`. Se applica, il ciclo procede e la `GateFn` valuta solo i budget locali. Se non applica, il ciclo nega in blocco e la corsia parcheggia fino alla scadenza della lease. Alla fine dello **stesso ciclo** un `incr` negativo con `min: 0` restituisce la differenza fra riservato e ammesso.

Il dettaglio che lo rende corretto, e che va scritto accanto all'implementazione: **il rimborso non deve dipendere dallo stato.** Riserva S, ammette A ≤ S, rimborsa S−A subito, tutto calcolato in memoria dentro il ciclo. Un ciclo interamente negato scarta le scritture di stato: se la contabilità del rimborso vivesse lì, la spesa su kv resterebbe e il rimborso sparirebbe, e la deriva sarebbe permanente e invisibile.

Costo dichiarato: due chiamate kv per ciclo (a `leaseSeconds: 1` sono ~2/s per corsia, sotto il tetto di scritture di queen con margine); e se il processo muore fra riserva e rimborso, `S−A` resta speso fino alla rotazione della finestra.

#### 3.5.2 `enforcement: feedback` — nessuna chiamata nel percorso

L'uso condiviso si aggrega dai meter di tutti i target membri e il cap efficace di ognuno viene ridotto perché la somma resti sotto il tetto. È **lo stesso meccanismo di `ceiling-minus-measured`** (§4.1), applicato a scope target invece che a scope corsia.

Costo dichiarato: un ritardo di una finestra di misura, durante il quale la somma dei membri può sforare.

**Come si sceglie.** `reserve` quando lo sforo è caro rispetto alla latenza aggiunta — il tetto per IP di Airbnb è il caso da manuale, perché la penale documentata è un blocco IP che colpisce l'intera flotta. `feedback` quando il tetto condiviso è largo rispetto alla domanda reale dei membri, e un ritardo di una finestra non basta a sfondarlo.

#### 3.5.3 Su kv la finestra è fissa, e questo vincola `alignment`

`incr` con TTL create-only implementa una **finestra fissa**: la riga si ricicla alla rotazione. Un budget condiviso `rolling` non è quindi esprimibile esattamente, e al confine di finestra accetta fino a **2× il tetto**.

Tre uscite, in ordine di preferenza:

1. dichiarare `alignment: calendar` e accettare che sia ciò che si sta implementando;
2. dichiarare `rolling` e dimensionare `cap` al 50–70% del tetto vero, con `confidence: inferred` e il motivo scritto in `source`;
3. l'approssimazione a due bucket — due chiavi kv e una lettura pesata, come fa `SlidingWindowGate` — che stringe molto e raddoppia le chiamate.

Il `PUT` **avverte** (non rifiuta) su `alignment: rolling` con `enforcement: reserve`, e l'avviso compare in `GET /v1/budgets/{id}`.

#### 3.5.4 La via che elimina il problema

Se si raggruppano i target per **identità di egresso** invece che per portale, il budget condiviso diventa un budget normale di quella partizione e il residuo è zero: i portali diventano una dimensione di `match` dentro lo stesso target.

Il prezzo va detto, perché non è gratis: la corsia è una sola, quindi il diniego del budget di un portale **parcheggia anche il lavoro degli altri portali** che condividono quell'egresso. Si scambia un budget condiviso esatto con un head-of-line fra portali. Conviene quando i portali che condividono il NAT hanno volumi molto diversi fra loro — quello piccolo non farà quasi mai parcheggiare quello grande — e non conviene quando sono comparabili.

### 3.6 `confidence` — e cosa ne fa `queen-rrl`

Non è editoriale: cambia il comportamento.

| Valore | Significato | Effetto |
|---|---|---|
| `documented` | il vendor lo scrive, con `source` e `asOf` | applicato al 100% |
| `inferred` | deduzione nostra da fonti reali | applicato al 100%, ma marcato nello stato |
| `assumed` | non lo sappiamo | applicato al **`assumedFactor`** (default 0,7) e sempre marcato |

`GET /v1/targets/{name}` riporta per ogni budget la confidence e la data: è ciò che rende i numeri contestabili, e ciò che impedisce che un `assumed` sopravviva due anni perché nessuno si ricordava che era un'ipotesi.

---

## 4. `Lane`

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `name` | `string` | sì | — | diventa un nome di partizione e di coda: `^[a-z0-9-]{1,32}$` |
| `cap` | `ceiling \| ceiling-minus-measured \| absolute:<n> \| share:<0..1>` | sì | — | §4.1 |
| `concurrency` | `int > 0` | sì | — | i consumer di quella corsia |
| `floor` | `0..1` | no | `0` | solo per `ceiling-minus-measured`: quota minima garantita |
| `default` | `bool` | no | `false` | esattamente una corsia deve averlo |

### 4.1 Le politiche di cap

- `ceiling` — la corsia può usare tutto il budget del target. Per la corsia urgente, il cui volume è piccolo e che non deve mai essere strozzata da una riserva indovinata.
- `ceiling-minus-measured` — cap = tetto meno il consumo misurato delle altre corsie nell'ultima finestra, con pavimento `floor`. È la corsia che assorbe ciò che le altre non usano. Il costo dichiarato è **un ritardo di una finestra di misura**, durante il quale le corsie insieme possono sforare: si compensa con un margine sul cap e una finestra corta.
- `absolute:<n>` — riserva statica. Semplice, e spreca ciò che la corsia non usa.
- `share:<f>` — frazione fissa del tetto. Come sopra, espressa in proporzione.

Le corsie sono **partizioni della stessa coda di push** e ognuna ha il suo gate pinnato, quindi budget, diniego e parcheggio sono indipendenti. Il diniego di una corsia non tocca l'altra.

---

## 5. `Cost`

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `field` | `string` | sì | — | il campo dell'item di lavoro che porta il costo |
| `default` | `int > 0` | sì | — | quando il campo manca |
| `max` | `int > 0` | sì | — | il costo massimo di un singolo item |

Il budget è denominato in **chiamate HTTP**, e un item ne produce N: un push di calendario tocca N listing e l'adapter emette una chiamata per listing. Contare messaggi significa far rispettare il limite sbagliato.

`max` non è documentazione, è un **cancello di validazione**: se esiste un budget il cui `cap` è minore di `cost.max`, il `PUT` viene rifiutato. Senza questo controllo un item più costoso della capacità non è mai ammissibile, blocca la testa della sua corsia **per sempre**, e non finisce mai in DLQ perché la scadenza della lease non consuma budget di retry. Sarebbe un blocco permanente e silenzioso, ed è il modo più probabile in cui questo sistema si rompe in produzione.

---

## 6. `Pacing`

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `leaseSeconds` | `int ≥ 1` | no | `1` | il quanto di pacing **e** la finestra di failover |
| `batch` | `int > 0` | no | `200` | deve essere ≥ della capacità per ciclo, o è il batch a limitare invece del budget |

`leaseSeconds` è in secondi interi e il minimo è 1: sotto il secondo non è esprimibile. Ed è due cose insieme — il ritmo con cui il lavoro negato ritorna, e il tempo in cui una corsia resta senza ammissione se muore la replica che la esegue. Entrambe spingono verso il basso, quindi il default è il minimo.

---

## 7. `Admitted`

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `partitionBy` | `connection \| entity \| none` | no | `connection` | come si ripartiziona il lavoro ammesso |
| `partitions` | `int > 0` | no | `64` | il parallelismo massimo dei consumer di quella corsia |

Una coda `admitted` **per corsia**: se entrambe emettessero nella stessa, la distinzione di corsia si perderebbe a valle e un ack urgente finirebbe nella stessa pila di un upload di foto.

`partitions` è il tetto vero al parallelismo dell'esecuzione: N consumer su una corsia con 8 partizioni ottengono 8, non N.

---

## 8. `Exclusive` e `Breach`

### 8.1 `Exclusive` — la mutua esclusione per entità

```
exclusive:
  - { match: { op: [...] }, by: entity }
```

Non è un rate ed è la ragione per cui non vive nei budget: Airbnb ammette **una mutation in volo per listing**, Holidu **un update per appartamento**. Si ottiene partizionando il lavoro ammesso per hash dell'entità, così la lease esclusiva di partizione è il mutex — nessun lock distribuito, nessun fencing token da presentare a un vendor che non lo accetterebbe.

Dichiarare `exclusive` implica `admitted.partitionBy: entity`. Con `partitions` finito, entità diverse possono cadere nello stesso bucket: si ottiene **più** serializzazione del necessario, mai meno, ed è l'unica forma sicura di ignoranza su una cardinalità che nessuno ha misurato.

Il caso che questo **non** copre è il lock **cross-endpoint** di Holidu, dove un push tariffe e un push foto sullo stesso appartamento collidono pur essendo op diverse e potenzialmente corsie diverse. Quello richiede un lock vero su `kv`, e va dichiarato a parte.

### 8.2 `Breach` — la tassonomia dello sforo, come dato

```
breach:
  - { when: { status: 429 }, outcome: throttled }
  - { when: { status: 200, jsonPath: "...", equals: "RATE_LIMIT_EXCEEDED" }, outcome: throttled }
  - { when: { status: 200, xpath: "...", contains: "6032" }, outcome: throttled }
```

Nove portali segnalano lo sforo in quattro modi diversi: 429 nudo, 429 con un codice GraphQL, un errore dentro un HTTP 200, un `responseCode` nel corpo JSON. Come codice sarebbero dodici classificatori da rideployare a ogni cambio di tassonomia del vendor; come dato sono una riga per regola, revisionabile senza deploy — che è quello che il documento di discovery chiede al §15.

`outcome` è chiuso: `ok | throttled | conflict | error`. Le regole si valutano in ordine e vince la prima.

**Dove girano.** Le applica l'SDK **lato chiamante**, così nessun corpo di risposta viaggia verso `queen-rrl`, ma sono **dichiarate qui** e scaricate col target: restano dato revisionabile centralmente senza che il chiamante rideployi.

### 8.3 `Observability`

I rollup sono sempre attivi e non si dichiarano: sono piccoli, aggregano sopra lo scope e vivono per anni (`PLAN_RRL.md` §16). Qui si dichiara solo ciò che costa: le tracce campionate e la cattura dei corpi.

```yaml
observability:
  traceSampleRate: 0.01           # ammissioni campionate; dinieghi e breach sono sempre interi
  capture:
    enabled: false
    when: non-2xx                 # non-2xx | sampled | all
    sampleRate: 0.05              # solo con when: sampled
    maxBytes: 32768
    retentionHours: 24
    redact:
      headers: ["authorization", "x-api-key", "cookie"]
      jsonPaths: ["$.guest.email", "$.guest.phone", "$..card"]
```

| Campo | Tipo | Obbligatorio | Default | Note |
|---|---|---|---|---|
| `traceSampleRate` | `0..1` | no | `0.01` | vale **solo** sulle ammissioni: dinieghi e breach sono sempre tracciati per intero |
| `capture.enabled` | `bool` | no | `false` | accendere significa archiviare dati di terzi: il nome del campo lo dice apposta |
| `capture.when` | `non-2xx \| sampled \| all` | no | `non-2xx` | `all` richiede `sampleRate` e viene rifiutato senza `redact` |
| `capture.maxBytes` | `int > 0` | no | `32768` | il troncamento è dichiarato nel record, mai silenzioso |
| `capture.retentionHours` | `int > 0` | no | `24` | separata dai rollup, così cancellare i corpi non tocca lo storico |
| `capture.redact.headers` | `string[]` | no | `["authorization","x-api-key","cookie"]` | i default **non sono sostituibili**, solo estendibili: una lista che rimuove `authorization` è rifiutata |
| `capture.redact.jsonPaths` | `string[]` | no | `[]` | applicati prima della scrittura, non alla lettura |

Non è un interruttore di debug: è la dichiarazione di quanto dato altrui questo target archivia, e per quanto.

---

## 9. Validazione al `PUT`

Ogni regola trasforma un guasto silenzioso in un rifiuto. È l'unica parte di questa spec che vale più della sua documentazione.

| # | Regola | Il guasto che previene |
|---|---|---|
| 1 | esiste ≥ 1 budget e ≥ 1 lane | un target che non limita niente |
| 2 | esattamente una lane ha `default: true` | item senza corsia, instradati a caso |
| 3 | per ogni budget: `cap >= cost.max` | blocco permanente e silenzioso della testa di una corsia |
| 4 | `pacing.batch >= max(cap)` per la corsia | è il batch a limitare invece del budget, e il tetto non si raggiunge mai |
| 5 | `store: gate` ⟹ `maxKeys <= soglia di cella` | un documento di stato che cresce senza freno e viene riletto a ogni ciclo |
| 6 | `scope` non vuoto ⟹ `maxKeys` presente | cardinalità non dichiarata, quindi non verificabile |
| 7 | ogni dimensione di `scope` è nell'insieme ammesso | contatori chiavizzati su attributi che nessuno stampa |
| 8 | `confidence: documented` ⟹ `source` e `asOf` presenti | un numero senza fonte, che è peggio di una lacuna dichiarata |
| 9 | `exclusive` non vuoto ⟹ `admitted.partitionBy: entity` | esclusione dichiarata e non ottenuta |
| 10 | `version` incrementata se cambia un campo di classe migrazione | budget azzerato in silenzio (§10) |
| 11 | ogni `budget.id` univoco | dinieghi e metriche non attribuibili |
| 12 | `capture.when: all` ⟹ `sampleRate` e `redact` presenti | un archivio integrale di corpi di terzi acceso con una riga |
| 13 | `redact.headers` contiene almeno i default | una lista che rimuove `authorization` archivierebbe le credenziali |

---

## 10. Versioning e le tre classi di cambiamento

| Classe | Campi | Comportamento del `PUT` |
|---|---|---|
| **A caldo** | `cap` di un budget, `cap`/`concurrency`/`floor` di una lane, `breach`, `confidence`, `source`, `asOf`, `assumedFactor` | applicato subito, nessun wipe: i parametri del gate non entrano nel config hash |
| **Drena e riavvia** | `pacing`, `admitted.partitions`, aggiunta di una lane, aggiunta di un budget | i runner si fermano, la corsia drena, ripartono |
| **Migrazione** | `period`, `alignment`, `scope`, `store` di un budget esistente; `admitted.partitionBy`; rimozione di una lane; `name` | **richiede `version` incrementata**. Il target vecchio resta vivo finché non è drenato, il nuovo parte accanto |

La ragione della terza riga è meccanica: cambiare `partitionBy` cambia i `partition_id`, e un `partition_id` nuovo è un contatore che **riparte da zero**. Farlo con un `PUT` in-place significa un limitatore che riparte a serbatoio pieno esattamente mentre stai cambiando i limiti perché qualcosa non andava. Cambiare `period` o `alignment` cambia il *significato* dello stato accumulato, che è lo stesso problema con un nome diverso.

---

## 11. Cosa non è esprimibile, dichiarato

1. **Priorità dentro una corsia.** Le corsie sono grosse: urgente contro bulk. Dentro una corsia il diniego è cieco alle sotto-chiavi, quindi non si dà precedenza a un tenant o a un flusso su un altro.
2. **Equità fra tenant.** Serve che il tenant arrivi al punto di push, e per `channel-go` oggi non ci arriva. Fino allora l'equità si approssima con `scope: [connection]`.
3. **Tetti di concorrenza puri** (max N in volo verso il vendor) come budget: si esprimono solo come `lane.concurrency`, cioè per corsia, non per chiave.
4. **Pacing sotto il secondo.** `leaseSeconds` è intero e il minimo è 1.
5. **Budget il cui costo si conosce solo dopo la risposta.** Si ammette su stima e si riconcilia col meter; il vero costo non può bloccare la chiamata che lo produce.
6. **Batching semantico.** Fondere due intervalli di date o aliasare cinque mutation in una richiesta richiede semantica di payload: è del chiamante, e questa spec non lo sa fare. È anche la leva a più alto rendimento del sistema, perché riduce la domanda invece di ritardarla.

---

## 12. Decisioni aperte sulla spec

| # | Decisione | Default proposto | Perché non è chiusa |
|---|---|---|---|
| 12.1 | `op` è una stringa a segmenti con glob sul suffisso, o un enum dichiarato nel target | **stringa con glob**, e un `op` non dichiarato è un errore al push | l'enum darebbe validazione totale al `PUT` ma costringe a versionare il target a ogni operazione nuova |
| 12.2 | `assumedFactor` è per target o globale | **per target, default 0,7** | un fattore globale è più semplice ma tratta allo stesso modo un'ipotesi su un tetto largo e una su un cap da 25 a settimana |
| 12.3 | Il lock cross-endpoint (Holidu) si dichiara qui o è fuori scope della v1 | **fuori scope**, con una nota nel documento | non abbiamo ancora un adapter Holidu, quindi il caso più difficile di C6 non ha un percorso da limitare e la forma verrebbe indovinata |
| 12.4 | `breach` valutato lato chiamante o lato `queen-rrl` | **lato chiamante**, regole scaricate col target | lato server sarebbe più centrale ma farebbe viaggiare corpi di risposta, con tutto ciò che comporta su log e privacy |
| 12.5 | ~~Il target di tipo `egress`~~ **chiusa**: è una risorsa a sé, `PUT /v1/budgets/{id}`, referenziata dai target | — | i target referenziano, così aggiungere un membro non modifica il budget condiviso |
| 12.7 | Il default di `enforcement` sui budget condivisi | **`reserve`**, perché la classe di guasto che coprono è cara (un blocco IP colpisce l'intera flotta) e il costo è due chiamate kv per ciclo | `feedback` costa zero nel percorso ma ha un ritardo di una finestra, e su un tetto stretto quella finestra basta a sfondarlo |
| 12.6 | La soglia di cella per `store: gate` | **da misurare**, non da indovinare | nessuno ha mai misurato il costo di rilettura di un documento di stato grande, e questa spec non deve congelare un numero che verrà da un banco |
