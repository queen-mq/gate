# PLAN — Gate: il reverse rate limiter come gate di streaming su QueenMQ

Rev 0.3 del 2026-08-18. Il progetto si chiama **Gate**. Il core, l'API target, l'auth, la console e lo storico sono costruiti e misurati; questo documento resta il registro delle decisioni.

Rev 0.2 aveva tolto l'archeologia dell'indagine — le alternative valutate e scartate, il registro delle correzioni, le giustificazioni di decisioni ormai chiuse. Resta la forma decisa, le regole che la rendono corretta, e ciò che manca.

Rev 0.3 registra una deviazione e il suo rientro: i rollup erano stati implementati su una coda queen invece che sulle tabelle che il §7 prescriveva. Con una sola replica funzionava; con più repliche no, ed è il §7 originale ad avere ragione (§9, decisione chiusa 12).

Risolve contro: `queen` @ branch `kvtimer`, HEAD `68f7f6b8` più il working tree (F4 non committata, `026_kv_sweeper.sql` untracked); `channel-go` @ `8c5eee1`. **Le citazioni `file:riga` si muovono**: quell'albero è in lavorazione, e vanno ricontrollate a campione prima di trattarle come contratto.

**I quattro documenti.** *Reverse Rate Limiter (RRL) — Discovery dei limiti OTA* è la verità sui **limiti**: nessun numero vive qui. `PLAN_KV_TIMERS.md` è la verità sulle **primitive** kv e timer. `TARGET_SPEC.md` è il **contratto** di configurazione. Questo decide la **forma** e tiene il registro delle decisioni.

---

## 0. Risposta in dieci righe

- L'RRL è un **gate di streaming**: `.Gate(ammetti).ToPartitioned(admitted)`, con la chiamata HTTP **a valle** in un consumer normale.
- La serializzazione che rende corretto un contatore di flotta è **gratis**: la lease esclusiva di partizione dà un solo scrittore, quindi le finestre annidate si valutano in memoria senza atomicità distribuita.
- **Il dominio di budget è la partizione, non la coda.** Una partizione per corsia, uno stream pinnato per corsia.
- **La chiamata OTA non può stare dentro il ciclo**: effetti prima del commit, seriali, senza rinnovo di lease. Fuori dal ciclo, e il vincolo sparisce.
- Il differimento del lavoro negato è **già implementato**: partial-ack, lease ritenuta, riconsegna FIFO, fatturazione zero.
- Il **quanto di pacing è la lease**, minimo 1 secondo, ed è anche la finestra di failover.
- L'unità di budget è la **chiamata HTTP**, non il messaggio: gate a costo pesato, costo timbrato dal produttore.
- A `queen.kv` resta **un solo** budget: quello che attraversa i target.
- Due difetti in queen/SDK stanno sul ramo di deny, cioè dove l'RRL vive. Vanno chiusi prima del primo traffico.
- Nessun percorso è **misurato** per la decisione di ammissione. Nessuno dei due lo era.

---

## 1. La forma

```
                       produttori (via SDK)
              partitionKey = target:corsia, costo timbrato
                               │
         gate.{app}.{t}.push — UNA partizione PER CORSIA
                    ┌──────────┴──────────┐
                 urgent                  bulk
                    │                     │
  stream "gate.{app}.{t}.urgent"  stream "gate.{app}.{t}.bulk"
        pinnato con .Partition()    pinnato con .Partition()
          .Map(costo)                 .Map(costo)
          .Gate(finestre)             .Gate(finestre)
          .ToPartitioned(byConnID)    .ToPartitioned(byConnID)
                    │                     │
  gate.{app}.{t}.admitted.urgent  ….admitted.bulk
             — N partizioni            — N partizioni
                    │                     │
          pool consumer urgent      pool consumer bulk
                    └──────────┬──────────┘
                        chiamata HTTP
```

**Due stadi, e la separazione non è estetica.** Il primo è CPU più due round trip e vive dentro la finestra di lease; il secondo fa I/O lento e ha bisogno di concorrenza, rinnovo di lease e timeout — tre cose che il runner degli streams non ha e non può avere.

**Uno stream per corsia, pinnato.** `Pop()` mette la partizione nel path (`clients/client-go/queue_builder.go:197-206`) e `AsStreamSource()` avvolge il `QueueBuilder` senza che nessun metodo di `Source` tocchi `partition` (`streams_adapter.go:78-80`). Quindi ogni corsia ha runner, batch, stato, budget e parcheggio propri.

**Una `admitted` per corsia.** Con una sola coda a valle la distinzione di corsia si perde, `rrl.Consume(target, lane, …)` diventa inesprimibile, e un ack urgente finisce dietro un upload di foto.

### 1.1 La mappa delle primitive

| Primitiva | Nell'RRL | Vincolo |
|---|---|---|
| `.Gate(fn)` + stato per `(query_id, partition_id, key)` | l'ammissione, con un solo scrittore garantito dalla lease | C1, C2, C3, C4, **C14** |
| partial-ack sul deny + lease ritenuta | il differimento, a fatturazione zero | **C15** |
| lease della coda sorgente | il quanto di pacing, per portale | C3, C17 |
| `.ToPartitioned(admitted, byConnID)` | parallelismo e ordine per connessione a valle | ordinamento |
| coda a valle partizionata per entità | mutua esclusione per listing / appartamento | C6 |
| `WindowCron` su uno **stream separato** | misura e tasso d'errore per finestra | C8, C11, C20 |
| `queen.kv` `incr` con `max`, fuori banda | il solo budget che attraversa i target | C7 |
| `queen.kv` `putIfAbsent` + TTL + `expect` | il lock cross-endpoint di Holidu | C6 |

**Cosa non si usa:** `.Foreach()` con effetti esterni (§2, condizione 1); i timer sul percorso di diniego, che è già gratis — restano per cose *O(domini)*, un pacemaker per dominio saturo e il TTL dei lock; il transaction wire per l'ammissione, perché ammissione e ack sono già atomici dentro `log_streams_cycle_v1`; `KeyBy` per l'equità, che dà isolamento di *stato* e non di *ammissione*.

---

**Il namespace è dell'applicazione.** Ogni coda che Gate crea è configurata con
`namespace = gate.{application}` (`QueueBuilder::namespace`, portato da
`configure`). Il prefisso puntato sul nome porta già l'applicazione, ma è una
convenzione che questo codice si è inventato e che il broker non legge: il
namespace è un campo che il broker tiene, quindi la console di queen filtra per
quello e chi guarda un broker condiviso vede le code di un team senza sapere
come Gate scrive i nomi. Una coda già esistente ci si sposta dentro alla
riconfigurazione, che avviene a ogni boot e a ogni `PUT`.

## 2. Le sette condizioni di ammissibilità

Il gate regge l'ammissione **se e solo se** valgono tutte insieme. Ognuna ha una verifica meccanica.

| # | Condizione | Verifica |
|---|---|---|
| 1 | La chiamata HTTP sta **fuori** dal ciclo: il gate termina con `.To()`/`.ToPartitioned()`, mai `.Foreach()` | nessun `ForeachOperator` nella catena; il codice che chiama vive in un handler `Consume` |
| 2 | La coda ha **una partizione per corsia**: `partitionKey` è `target:corsia`, mai la connessione | `SELECT count(DISTINCT partition_id)` = numero di corsie |
| 3 | Il gate ammette a **costo pesato**, e `capacity >= cost.max` | un test che spinge il messaggio col fan-out più grande osservato |
| 4 | Prima del gate **solo `Map` e `KeyBy`** — mai `Filter`, mai `FlatMap` | un test che mette un filter davanti al gate, forza un deny e legge `log_consumers.committed` |
| 5 | L'ack parziale è idempotente al retry, **oppure** il retry su `/streams/v1/cycle` è disabilitato con `release_lease=false` | chiamare `log_streams_cycle_v1` due volte con lo stesso body e asserire che `committed` avanza una volta |
| 6 | La lease è **deterministica** e tarata per portale: `capacity = TPS × leaseSec`, `batch >= capacity` | leggere `queen.queues.lease_time` **dopo un riavvio completo** e vedere il valore atteso |
| 7 | La `GateFn` è **pura** in *(stato persistito, `StreamTimeMs`)* e non muta lo stato sul deny | un test che nega l'intero batch e asserisce che nessuna riga di `queen_streams.state` è cambiata |

La 5 e la 6 non sono nostre: sono difetti e lacune di queen (§5). Le altre cinque sono disciplina dell'RRL.

**Perché la 1.** `buildSink` (`runner.go:591`) esegue gli effetti in un `for` seriale senza timeout (`operators.go:255-265`) e `CommitCycle` è a `:629`: gli effetti girano **prima** del commit, e il percorso streams non rinnova mai la lease. Alla scadenza il check solleva (`007_log_streams.sql:340-350`) dentro un blocco con `EXCEPTION WHEN OTHERS` (`:456-467`): rolla back stato, sink e ack, mentre le chiamate **sono già partite**. Il budget registra zero, il cursore non si muove, lo stesso batch torna — livelock con chiamate duplicate. E anche senza il guasto, effetti seriali nel ciclo danno `1/latenza` per runner: 3,3 chiamate/s a 300 ms. Il costo della regola è una coda in più per corsia; il codice richiesto è zero.

**Perché la 3.** Un messaggio non è una chiamata: un push di calendario tocca N listing e l'adapter emette una chiamata per listing. Il moltiplicatore è intrinseco al fan-out, non ai retry, quindi togliere i retry interni non lo elimina. Due conseguenze: `capacity >= cost.max`, altrimenti quel messaggio non è **mai** ammissibile e blocca la testa della corsia per sempre senza mai finire in DLQ (la scadenza della lease non addebita retry, `004_log_pop.sql:49-52`); e ogni retry di adapter non contato nel costo è budget perso in silenzio.

---

## 3. Le regole della `GateFn`

Meccaniche, non stilistiche, e nessuna è documentata da queen oggi. Sono la parte di questo documento che vale anche fuori da qui.

1. **Pura in *(stato persistito, `StreamTimeMs`)*.** Nessuna I/O, nessun orologio proprio, nessuna randomicità. Un ciclo interamente negato ritorna prima di `CommitCycle` (`runner.go:571-576`) e scarta ogni scrittura di stato; un effetto fuori dallo stato non viene scartato e divergerebbe per sempre.
2. **Idempotente sulla ri-valutazione.** Gli stessi messaggi tornano a ogni scadenza di lease. Scrivi valori **assoluti** derivabili da *(stato caricato, now)*, mai incrementi relativi.
3. **Non mutare lo stato sul deny.** In Go, JS e Python non esiste rollback per messaggio (solo Rust lo fa): la `GateFn` riceve la mappa **viva** e su deny il ciclo fa solo `break`, quindi la scrittura di un messaggio negato viene persistita se quella chiave era già stata toccata da un allow. La documentazione promette il contrario ed è sbagliata su tre SDK su quattro.
4. **Prima del gate solo `Map` e `KeyBy`.** `ack.count` è contato in **envelope** dal client e interpretato in **frame** dal server: un `Filter` fa avanzare il cursore di meno frame di quelli consumati (duplicati), un `FlatMap` di più (messaggi saltati). Si manifesta **solo** sul deny.
5. **Mai un `Filter` dopo il gate.** `allowedCount` è calcolato prima dei post-stage e l'ack lo usa comunque: un emit scartato a valle ha già bruciato il token.
6. **`capacity = rate × leaseSeconds`, `batch >= capacity`.** Le ammissioni per ciclo sono `min(batch, token all'inizio del ciclo)`, perché `StreamTimeMs` è campionato una volta prima del loop.
7. **`StreamTimeMs` è l'orologio del processo client**, non del broker. Non usarlo per finestre che devono coincidere fra processi.
8. **Nessuna chiave di stato che inizi con `__`.** Go non valida e `__wm__` è del runtime.
9. **Mai restituire errore per una condizione di business.** Un errore di valutazione fa `return nil` senza ack: il batch torna indefinitamente, senza consumare retry budget e senza mai raggiungere una DLQ. L'unico esito legittimo è un booleano.
10. **Ogni chiamata uscente porta una `Idempotency-Key` deterministica.** Non esiste at-most-once: l'ordine è sempre effetti-poi-ack, e l'ack parziale può riavanzare il cursore al retry.
11. **Il break al primo deny è per gruppo-partizione ed è cieco alle sotto-chiavi.** Dentro una partizione, un deny sulla chiave A ferma anche B..K che hanno ancora token.

Una proprietà, non una regola: i parametri del gate **non entrano nel config hash** (`GateOperator.Config()` ritorna `nil`), quindi i cap si ritarano **a caldo** senza wipe dello stato. È ciò che C20 chiede, e viene gratis.

---

## 4. Il contratto di wire

L'SDK è **sottile sopra HTTP**: un solo contratto, tutti i linguaggi gratis, e il chiamante non vede mai una coda, una partizione o un offset — vede un target, una corsia, un item e una lease. La forma dichiarativa del target vive in `TARGET_SPEC.md`.

| Verbo | Semantica | Sotto |
|---|---|---|
| `PUT /v1/targets/{t}` | dichiarativo, idempotente, restituisce la topologia risolta | `POST /api/v1/configure` per corsia, `POST /streams/v1/queries` per gate |
| `POST /v1/targets/{t}/lanes/{l}/push` | `{key, cost, txn, payload}`, con variante batch | `POST /api/v1/push` |
| `GET /v1/targets/{t}/lanes/{l}/next` | long poll; se il gate non ammette **non ritorna**, e l'attesa è il pacing | pop sulla `admitted` della corsia |
| `POST /v1/leases/{id}/renew` | per le chiamate lente | `POST /api/v1/lease/{id}/extend` |
| `POST /v1/leases/{id}/ack` | `{upTo: N, calls: N, outcome, retryAfterMs?}` | **una sola** `POST /api/v1/transaction` |
| `POST /v1/leases/{id}/nack` | non tentato: torna in corsia e il budget è rimborsato | ack `ok=false` + `incr` negativo con `min:0` |

**L'ack e l'evento di misura committano insieme.** Con due chiamate separate, se l'ack passa e il push dell'evento no, il lavoro è ackato ma la spesa non è contata: il meter sotto-conta, il cap efficace **sale**, e il limitatore crede di avere budget che non ha — sfora in silenzio proprio quando sta già andando male. Il meccanismo esiste ed è committato: `POST /api/v1/transaction` (`main.rs:830`) → `queen.log_transaction_wire_v1` (`005_log_ack.sql:1108`), *atomic push+ack*, esposta dall'SDK Go a `transaction_builder.go:157`. Non richiede F4.

**L'ack non è contabilità, è l'anello di retroazione.** Porta il numero **reale** di chiamate — al push era una stima — e l'esito nella tassonomia del portale. Un chiamante che ackka senza esito lascia il limitatore cieco, e va rifiutato in validazione.

**La distinzione che decide il rimborso:** `nack` significa *non tentato*, quindi il budget del vendor non è stato consumato e va restituito. Un `ack` con `outcome: throttled` significa *tentato e rifiutato*: la richiesta è partita, il vendor l'ha contata, il budget **non** si rimborsa — ed è il segnale più informativo che riceviamo, perché in un sistema open-loop è l'unico feedback reale.

**Nessuna affinità, in nessun punto.** L'ack è `log_ack_v1(queue, partition, group, worker, upTo, …)`: tutto viaggia nella richiesta, la lease è un token in Postgres e non una sessione, quindi il `leaseId` della replica A si ackka alla replica B. Anche il gate è senza affinità: ogni replica avvia un runner per ogni corsia e la lease decide chi lavora — qualunque ownership orfana le corsie di una replica morta, che è il guasto peggiore possibile. L'affinità farebbe danno: con sticky session, una replica che muore rende l'ack non instradabile e il lavoro aspetta la scadenza.

**La trappola operativa da scrivere nell'SDK:** il long poll contro l'idle timeout del load balancer. Il `wait` di default deve stare con margine sotto il timeout del proxy, e una connessione tagliata è "nessun lavoro", mai un errore.

**Alternativa scartata:** far decidere il gate al momento della `next`, eliminando la `admitted`. È la semplificazione che verrà riproposta: sposta l'ammissione nel percorso della richiesta, perde l'accoppiamento transazionale del ciclo, e soprattutto elimina la proprietà per cui un diniego **parcheggia la partizione** — che è ciò che rende il differimento gratuito.

**Disponibilità.** `queen-rrl` sta sul percorso di ogni chiamata uscente: se è giù, non esce niente. Non è un punto di guasto nuovo — il percorso uscente dipende già da queen — quindi il rischio marginale è il solo processo, che le repliche coprono. Il residuo che le repliche non coprono, con il suo numero:

> **Il tempo di failover dell'ammissione di una corsia è la durata della sua lease.**

La lease è quindi insieme il quanto di pacing e la finestra di failover, e le due cose spingono nella stessa direzione: lease corte sono doppiamente buone.

**Invariante di proprietà.** Le code di `queen-rrl` **non passano da `EnsureQueue` di `channel-go`**, che applica un upsert full-config a ogni coda che consuma (`topics.go:229-233`, `queen.go:521-541`) e sovrascriverebbe la lease al primo rolling restart. Va reso meccanico con un test che fallisce se un nome `rrl.*` compare in quella lista.

---

## 5. Le dipendenze da queen, e i due difetti bloccanti

### 5.1 Il retry dell'ack parziale riavanza il cursore

L'exactly-once di `007:95-101` poggia sul fatto che al retry la lease sia sparita. Ma il ramo parziale (`007:384-431`) **ritiene** `worker_id` e `lease_expires_at` (`:424-430`), che sono esattamente ciò che il lease check guarda — e il client ritenta il `POST /streams/v1/cycle` fino a 3 volte. Un timeout di rete sul commit di un ciclo con deny fa camminare il cursore di altri K frame: **K messaggi consumati e mai inviati**, perdita silenziosa. Il budget non si addebita due volte (gli upsert sono idempotenti): si perde lavoro. Latente a basso carico, si attiva quando il portale satura, cioè quando l'RRL serve.

*Fix minimo:* non ritentare quando `release_lease=false`. *Fix robusto:* fencare l'ack parziale con `ack.transactionId`, che il client calcola già e il server butta. *Prima di entrambi:* il test, che non esiste.

### 5.2 `ack.count`: envelope contro frame

Regola 4 di §3. **Rust ha già corretto** (`group_by_source_message`, con test; CHANGELOG `1.0.0-beta.3`); Go, JS e Python no. Corollario: un `Filter` pre-gate che scarta l'intero batch produce un livelock, perché il percorso non-gate acka `len(ordered)` mentre il gate no.

### 5.3 Cosa resta a `queen.kv`

Lo stato del gate è per `(query_id, partition_id, key)`: due target sono due query e non condividono una riga. Quindi **un budget che attraversa i target non è applicabile da nessun gate**. Il meccanismo, i due modi di enforcement e i loro costi sono in `TARGET_SPEC.md` §3.5.

Due fatti che non stanno lì. **Lo stato di kv oggi:** `QUEEN_KV_ENABLED` è `false` di default (`config.rs:1031`), `rate_check` è uno stub che ritorna `None` (`handlers/kv.rs:171`), `kv_pool` ritorna il pool condiviso (`:153-159`, *"NOT yet a bulkhead"*), `026_kv_sweeper.sql` è untracked, e zero SDK espone kv. **Un difetto da segnalare a prescindere dall'RRL:** nel ramo di fallback per chiave assente (`024_kv.sql:1248-1256`) il `reason` è hardcodato a `'limit'` senza discriminare `'type'`, quindi un limitatore tratterebbe un errore di configurazione come budget esaurito e aspetterebbe per sempre sul motivo sbagliato.

### 5.4 Gli ask, ordinati per valore

| # | Ask | Dove | Costo |
|---|---|---|---|
| 1 | `ToPartitioned(sink, partition fn)` in Go — `PartitionResolver` e `resolvePartition` esistono, `To()` non li imposta, e il config hash non cambia perché `Config()` emette solo `{kind, queue}` | sdk | ~5 righe, parità con gli altri tre |
| 2 | Fix del retry dell'ack parziale (§5.1) | queen | una condizione client-side, o il fencing server-side |
| 3 | `leaseSeconds` per-pop — il server accetta già `?leaseSeconds=N` (`data.rs:601-603`), il client non lo emette | sdk | 2 righe, un metodo sul builder, uno sull'interfaccia, un campo in `RunOptions` |
| 4 | `group_by_source_message` in Go, JS, Python | sdk | ~40 righe più test |
| 5 | Rollback per messaggio sul deny in Go — rende strutturale la regola 3 | sdk | ~15 righe |
| 6 | `TransactionID` su `PushItem` in Go — senza, il dedup del sink non scatta mai | sdk | piccolo, attenzione al dedup per segmento |
| 7 | Tre test sul ramo di deny — oggi non è esercitato da nulla | queen | tre test |
| 8 | Committare `026_kv_sweeper.sql` | queen | zero |
| 9 | Paratia e token bucket kv — solo se serve il budget condiviso | queen | i due seam di `PLAN_KV_TIMERS` §8.4 |

Nessun ask riguarda lo schema del broker. **Il percorso critico non passa da F4.**

---

## 6. Il wiring in channel-go

**Il cambio è una riga per sito di push.** `ProduceRaw(ctx, topic, partitionKey, transactionID, data)` è uniforme e `portalKey` è già il primo parametro di `enqueuePush` (`orchestrator.go:1087-1101`); i siti sono quello più i gemelli in `contentsync`, `ratecheck`, `pollscan`, `listingactions`, `connectionactions`, `messaging`. Il dedup non si rompe — è per partizione su `transactionId` e lo schema `{correlationId}:{connId}` resta distinto; allargare la partizione **allarga** il dedup.

**I numeri non esistono nel codice.** `ota.Direction.RateLimit` (`internal/ota/portal.go:73`) è vuoto per tutti gli **11** portali, mentre `channel.ip_rate_limits` (`migrations/0001:1431`) e `channel.api_rate_limits` (`:1446`) esistono in produzione, sono migrate al cutover e nessun codice Go le legge. `portals.enable_rate_limits` (`:1339`) è CRUD-abile e mai consultata: è il kill switch per portale, gratis. Nota: `booking-com` non ha push (`portal.go:41-53`), quindi le code sono 11 e il tetto Booking non ha nulla da ritmare.

**Da rimuovere, non affiancare:** il retry di VRBO che ritenta sul 429 (`vrbo/client.go:289-292`) e la perdita silenziosa di `checkAllFailed` (`vrbo/vrbo.go:274-279`). Tre righe e un cambio di classificazione, non servono a nessun limiter, **e sono il primo incremento**.

**Lo stream della misura è separato**, perché `.Gate()` è incompatibile a compile-time con window e reduce: un secondo stream sulla stessa coda con il suo `queryID`, `WindowCron('minute')` più `Aggregate`. Oggi il segnale `rate_limit` copre circa un ottavo del traffico uscente, perché `ota.WithCallScope` è timbrato in un solo punto (`orchestrator.go:938`).

---

## 7. Osservabilità

Quattro strati, perché "tenere tutto" mescola due cose con costo e rischio opposti.

| Strato | Cosa | Volume | Ritenzione |
|---|---|---|---|
| **1. Eventi** | un evento per chiamata su `gate.{app}.{t}.calls`: esito, status, latenza, costo stimato e reale, budget consultati con la loro utilizzazione | pari alle chiamate | retention della coda |
| **2. Rollup** | aggregati per `(application, target, lane)` a minuto | 10³–10⁴ righe/giorno | 90 giorni (`prune`) |
| **3. Tracce** | *perché* ammessa o negata: budget vincolante, corsia, op, esito | per decisione | 7 giorni (`prune`) |
| **4. Corpi** | richiesta e risposta | enorme | ore, opt-in |

Lo strato 1 esiste già. Lo strato 2 è lo stream misura con una destinazione durevole.

Gli strati 1–3 sono costruiti; lo strato 4 (`capture`) è dichiarabile nella spec e non ancora scritto.

**Il grafico unico.** `GET /api/flow` risponde, in una query sola per tutto il
deployment, una serie per applicazione: per ogni minuto l'utilizzazione del suo
target **più carico**, non la media. L'asse è l'utilizzazione e non le
ammissioni perché due applicazioni con tetti diversi non possono condividere un
asse assoluto — quella piccola sarebbe una riga piatta in fondo qualunque cosa
faccia, ed è quella che sta per essere rifiutata. La media fra i target
mostrerebbe un team con quattro target fermi e uno incollato al tetto come un
comodo 20%: il grafico porta quindi il nome del target responsabile, e il volume
resta sulle pagine di dettaglio.

**La regola di cardinalità.** Un budget `scope: [entity]` su 200.000 listing è legittimo e sarebbe una serie temporale suicida:

> **I rollup aggregano SOPRA lo scope.** Il dettaglio per chiave vive solo nelle tracce, campionato.

Dimensioni ammesse su una metrica: `target`, `lane`, `op`, `outcome`, `budget_id`. Mai `entity`, `connection`, `tenant`.

**Il campionamento non è uniforme**, perché le ammissioni sono il 99% del volume e lo 0% dell'interesse: ogni **diniego** intero; ogni **breach** intero più le N decisioni che lo precedono su quel budget; le ammissioni campionate.

**I corpi non sono il default**, e la ragione non è il costo: contengono dati degli ospiti e le intestazioni contengono credenziali. Opt-in per target, campionato, redatto, con tetto e troncamento dichiarato e ritenzione corta separata dai rollup — vincoli che il `PUT` applica (`TARGET_SPEC.md` §8.3). Il campo si chiama `capture` e non `debug` perché archivia dati di terzi, e il nome deve dirlo a chi lo accende.

**Le due misure che nessun altro terrebbe.** Il **tetto reale contro il dichiarato**: l'utilizzazione al momento di ogni breach dà, per budget, una stima del tetto vero, e la distanza dal `cap` è il debito di conoscenza del sistema — è ciò che trasforma il documento di discovery in qualcosa che si corregge da solo. Il **costo reale contro lo stimato**: la deriva per op dice se il modello di costo è rotto, e un modello di costo rotto sfonda in silenzio ogni budget TPS. Va allarmata su soglia, non guardata su un grafico.

**Dove vive.** Rollup e tracce sono tabelle di Gate nello stesso Postgres di queen (schema `gate`); la console legge quelle. Tracce e corpi vanno scritti da un consumer separato a concorrenza limitata, **mai in linea con l'ack**: non devono competere col percorso messaggi.

Lo schema si crea da solo al boot (`CREATE ... IF NOT EXISTS`), e le variabili sono `PG_HOST`, `PG_PORT`, `PG_USER`, `PG_PASSWORD`, `PG_DATABASE`. **Senza `PG_HOST` il gate parte lo stesso**: limita esattamente come prima e semplicemente non sa rispondere su ieri. È la scelta giusta per chi prova Gate in locale e la scelta sbagliata in produzione, dove la console mostrerebbe la storia di una replica sola.

```sql
gate.rollups(application, target, lane, minute) -- PK; upsert che SOMMA
gate.traces(id, at, application, target, lane, op, outcome, budget_id, calls)
```

**Perché la chiave primaria somma e non sovrascrive.** Ogni replica consuma una fetta degli eventi di chiamata, quindi ogni replica ha visto un pezzo del minuto. Due repliche che scrivono lo stesso minuto non sono una corsa da risolvere: sono due addendi. `admitted = gate.rollups.admitted + EXCLUDED.admitted` è l'unica forma che rende il totale indipendente da quante repliche ci sono e da come il broker ha diviso le partizioni.

**Cosa resta locale alla replica**, e va letto sapendolo: i contatori `admitted`/`denied` per corsia nella vista target sono del processo che risponde, non del deployment. Il numero da guardare in un deployment con più repliche è `admitted_per_sec`, che viene dalla tabella.

---

## 8. Cosa non copriamo, dichiarato

1. **Il batching semantico (C12).** È la leva a più alto rendimento perché riduce la domanda invece di ritardarla, e nessuna architettura valutata la consegna: fondere due intervalli di date o aliasare cinque mutation richiede semantica di payload che nessuno scheduler ha.
2. **Equità fra tenant (C13).** Non implementabile finché `CompanyID` non raggiunge il punto di chiamata (scartato in `syncwire.go:38-53`). Fino allora si approssima con `scope: [connection]`.
3. **Priorità dentro una corsia.** Fra corsie è coperta (due partizioni, due budget, due parcheggi); dentro una no, perché il break al primo deny è cieco alle sotto-chiavi.
4. **La deadline dei 30 minuti di Booking (C10).** Ogni meccanismo calcola lo slack dall'orologio locale, che non è il momento in cui è partito il timer del vendor. E la premessa non è localizzabile: Booking.com in channel-go non ha né pull né webhook né push.
5. **Nessun criterio di latenza esiste.** Zero occorrenze di "SLA" in channel-go, nessun codice misura la distanza da una deadline.
6. **La stima del costo va verificata contro il reale** prima di passare a enforce. Un limite di 20 TPS applicato a stime dichiarate non è un limite di 20 TPS.

---

## 9. Decisioni

### 9.1 Chiuse

| # | Decisione | Esito |
|---|---|---|
| 1 | Dove vive la chiamata HTTP | **A valle**, in un consumer piano. Porta a senso unico |
| 2 | `partitionKey` della coda di push | **`target:corsia`**, una partizione per corsia. Porta a senso unico |
| 3 | Il gate conta messaggi o chiamate | **Costo pesato**, timbrato dal produttore |
| 4 | Priorità fra urgente e bulk | **Due partizioni della stessa coda**, un solo stream per corsia pinnato, `urgent` a cap ≈ tetto e `bulk` a `ceiling-minus-measured` ritarato a caldo dal meter |
| 5 | Chi decide la classe di un flusso | **Il produttore, nella `partitionKey`**: greppabile e sottoposta a review, non modificabile a caldo. Il meter riporta la quota urgente, che la rende contestabile |
| 6 | La topologia si cabla o si dichiara | **Si dichiara.** Il chiamante nomina target e corsia, mai una coda. Porta a senso unico |
| 7 | Semantica del payload | **Per flusso.** Invalidazione per la sincronizzazione di stato, valore per gli eventi in cui ogni occorrenza conta. Porta a senso unico |
| 8 | `queen-rrl` sul percorso dati | **Fail-closed accettato** con repliche; via di fuga scartata |
| 9 | Linguaggio | **Rust**: è l'unico SDK che gestisce correttamente il ramo di deny, e il linguaggio è invisibile a chi usa l'API |

| 12 | Dove vive lo storico | **Tabelle `gate.rollups` e `gate.traces` nel Postgres di queen**, non una coda | Una coda è append-only e ordinata, che è la forma giusta per un rollup, e per una replica sola funziona. Ma il gruppo di consumo divide gli eventi fra le repliche: ognuna vedrebbe una fetta, ne aggregherebbe una parte, e la console risponderebbe una storia diversa a seconda del pod. E per ricostruire l'anello al boot ogni replica rileggeva la coda con un gruppo nuovo — 18 gruppi orfani sul broker prima che qualcuno guardasse. Il §7 diceva "tabelle" fin dalla prima stesura; l'implementazione era andata altrove |

### 9.2 Aperte

| # | Decisione | Default proposto | Scadenza |
|---|---|---|---|
| A | Correggere il doppio avanzamento del cursore (§5.1), o accettarlo | **Correggere**, e non spedire sul ramo non corretto | prima del primo traffico |
| B | Come si tara la lease per portale | **`leaseSeconds` per-pop** (ask 3): evita il pericolo dell'upsert full-config | due settimane; fino allora il quanto di pacing è sbagliato di due ordini di grandezza |
| C | Da dove vengono i tetti per portale | **Popolare `ota.Direction.RateLimit`** da `ip_rate_limits` e `api_rate_limits`, che esistono e nessuno legge | due settimane, in parallelo al prototipo |
| D | Testare il ramo di deny prima di spedire | **Testare.** Tre test: lease che scade con effetti lenti; ack parziale chiamato due volte; `Filter` davanti al gate più deny | prima del primo traffico |
| E | Accendere `QUEEN_KV_ENABLED` | **Non ancora**: serve solo per il budget condiviso, e kv è spento con `rate_check` stub e `026` untracked | quando serve il primo budget cross-target |
| F | Le sei decisioni di spec ancora aperte | vedi `TARGET_SPEC.md` §12 | con la prima implementazione |

---

## 10. La forma open source

Un servizio con un'API HTTP: chi lo usa non importa nulla, quindi il linguaggio è invisibile e il pubblico è chi usa QueenMQ. Il legame con queen non è un caveat da scusare — è l'offerta, ed è ciò che rende disponibili le tre proprietà che una libreria portabile non può dare: serializzazione gratis dalla lease, differimento gratis dal partial-ack, fusione gratis dal dedup.

- **Il nome pubblico non è "reverse rate limiter"**: non è terminologia di letteratura e collide con reverse proxy. La famiglia è *client-side / egress rate limiting*.
- **Prior art nel README**, in ordine: Doorman (il precedente diretto, archiviato dal 2024), Gubernator, `envoyproxy/ratelimit`, Bucket4j, Temporal e Inngest — che hanno l'80-90% ma come piattaforma di esecuzione.
- **Il contributo originale:** le regole di §3, che sono il contratto del percorso `.Gate()` che oggi nessuna pagina di queen dichiara. E il vincolo per-entità — una mutation in volo per listing — che **nessuna OTA documenta pubblicamente**: formalizzarlo sarebbe il primo artefatto pubblico che lo fa.
- **Cosa resta specifico e non si pubblica:** i documenti di configurazione con i numeri reali, i classificatori di breach per portale, i mapping di shaping.

---

## 11. Manutenzione

- I **numeri** vivono nella configurazione, mai qui. Due sorgenti di verità significa zero sorgenti di verità.
- Le **regole di §3** vanno anche nella webdoc di queen: non sono nostre, sono il contratto di `.Gate()`.
- Quando un ask di §5.4 atterra, la regola che lo compensava va **rimossa** da §3, non lasciata come cintura doppia.
- Quando le sette condizioni di §2 hanno tutte un test, questo documento diventa un README e le condizioni diventano la suite.
