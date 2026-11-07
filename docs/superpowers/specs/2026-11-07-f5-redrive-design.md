# F5 (parte 2): redrive

- Data: 2026-11-07
- Fase: F5 do `messaging-inbox/docs/plan.md`, segundo ciclo (o nível 3 foi o primeiro)
- Decisão base: ADR-0012 (redrive), ADR-0017 (fingerprint), ADR-0002 (cópias e `inbox_attempts`)
- Abordagem: A — redrive no core sobre um `PassReader` extraído do `DeadLetterPass`; o starter monta endereços,
  bean e endpoint

## Objetivo

Depois de corrigir um bug ou uma configuração, o operador inspeciona a `failed` ou a DLQ (agrupada por exceção e
fingerprint) e devolve só o grupo certo à fila principal (ou `failed` → DLQ), com as tentativas zeradas, num ritmo
que não derruba a dependência recém-recuperada. Mesmo procedimento em todos os provedores que permitem ler a origem.

## Fora do escopo

- Execução assíncrona (decidido: síncrona, como a ADR).
- CLI, transformação de payload, arquivamento externo (descartados na ADR-0012).
- Coordenação entre instâncias: uma execução por receiver e origem **por instância**.
- Ligar o endpoint no engine-tax (decisão de operação).

## Core

### API (`com.example.messaging.inbox.redrive`, públicos)

```java
public interface InboxRedrive { RedriveReport redrive(RedriveRequest request); }

public record RedriveRequest(String consumer, Source source, Target target, int limit,
                             Filter filter, int maxPerSecond, boolean dryRun) {}
public enum Source { DEAD_LETTER, FAILED }
public enum Target { MAIN, DEAD_LETTER }
public record Filter(String exception, String fingerprint, String reason,
                     Instant enqueuedBefore, Instant enqueuedAfter) {}   // todos opcionais; E entre eles
public record RedriveReport(String redriveId, PassEnd end, int moved, int skipped, int errors,
                            Map<String, Integer> byException, Map<String, Integer> byFingerprint) {}
```

`Filter.NONE` (todos `null`) é o padrão. O core exige `maxPerSecond` ≥ 1; o endpoint usa 100 quando o campo é omitido.

### Validação (antes de ler qualquer mensagem)

- `limit` em 1..10_000; `maxPerSecond` ≥ 1.
- Combinações aceitas: `FAILED → MAIN`, `FAILED → DEAD_LETTER`, `DEAD_LETTER → MAIN`. As demais: recusadas.
- `FAILED → DEAD_LETTER` exige DLQ no receiver; `DEAD_LETTER → *` exige DLQ.
- `Target.MAIN` exige destino principal enviável (fila). Receiver de tópico + subscription: recusado (publicaria
  para todas as subscriptions).
- Origem ilegível (Pub/Sub: tópico, não subscription): recusado com o motivo registrado no startup.
- Uma execução por receiver e origem ao mesmo tempo, por instância: a segunda é recusada.
- Erros de validação: `IllegalArgumentException` (pedido inválido), `NoSuchElementException` (receiver
  inexistente), `IllegalStateException` (execução em curso).

### `PassReader`

Extraído do `DeadLetterPass`, package-private no pacote `com.example.messaging.inbox`. A implementação do redrive
(`DefaultInboxRedrive`) fica no mesmo pacote para usá-lo; só a API (`InboxRedrive` e os records) fica em
`com.example.messaging.inbox.redrive`. Lê uma mensagem
por vez e para, devolvendo com `nack(0)` a mensagem que encerrou, quando: `enqueuedAt >= runStart`
(`NEWER_THAN_PASS`), chave (id do envelope, ou `messageId`) repetida (`REPEATED_KEY`), limite atingido
(`MAX_PER_PASS`), `receive` vazio (`EMPTY`), parada (`STOPPING`), falha de `receive` (`RECEIVE_FAILED`). O
`DeadLetterPass` passa a usá-lo sem mudar de comportamento (os testes dele seguem iguais).

### Fluxo de uma execução

1. `redriveId` = UUID; `runStart` = agora; log de início.
2. Para cada mensagem do `PassReader` sobre a origem, até `limit`:
   - Conta em `byException`/`byFingerprint` pelo `inbox_failure` (`FailureAttribute.parse`), para **todas** as
     lidas; sem `inbox_failure`, a chave é `none`.
   - Filtro: `exception` e `fingerprint` iguais aos do `inbox_failure`; `reason` é prefixo de
     `deadLetter().reason()` (ex.: `rejected`, `retries_exhausted`); `enqueuedBefore`/`enqueuedAfter` sobre
     `enqueuedAt` — mensagem sem `enqueuedAt` não passa num filtro de data.
   - Não passa, ou `dryRun` → cópia sem mudança para o **fim da origem**, depois `ack` (`skipped`).
   - Passa → espera o ritmo de `maxPerSecond` (só as movidas contam), envia a cópia ao destino, depois `ack`
     (`moved`). Cópia: `OutgoingMessage.copyOf` (corpo, `contentType`, `orderingKey`, `traceparent`, atributos que
     a cópia aceita), **sem** `inbox_failure`, com `inbox_attempts=l2=0;l3=0;redrive=<redriveId>`,
     `deduplicationId` = `messageId` de origem.
   - Falha ao copiar (destino ou fim da origem) → `nack(0)` da original, `errors++`, segue.
3. Relatório com `end`; log `INFO` de auditoria (id, receiver, origem, destino, filtro, `dryRun`, contagens, `end`;
   sem payload).

## Starter

### Bean

- `InboxRedrive` registrado pela `InboxAutoConfiguration` (só com a inbox ligada).
- Por receiver configurado (ligado ou não):
  - origem `FAILED`: receiver e sender `<receiver>.failed`;
  - origem `DEAD_LETTER`: receiver e sender `<receiver>.dead-letter` (com `dead-letter`);
  - destino `MAIN`: sender `<receiver>.redrive` sobre o `destination` do receiver, só quando ele é fila
    (`subscription == null`).
  - Legibilidade de cada origem validada no startup (`validate(..., receive=true)`); ilegível → indisponível com o
    motivo.

### Endpoint Actuator `inboxredrive`

- Só com o Actuator no classpath; acesso padrão **nenhum** (`defaultAccess = NONE`).
- Ligar exige `management.endpoint.inboxredrive.access=unrestricted` **e** `inboxredrive` em
  `management.endpoints.web.exposure.include`.
- `GET /actuator/inboxredrive`: receivers com origens e destinos disponíveis e, nos indisponíveis, o motivo.
- `POST /actuator/inboxredrive/{consumer}`: corpo com `source`, `target`, `limit`, `maxPerSecond`, `dryRun`,
  `exception`, `fingerprint`, `reason`, `enqueuedBefore`, `enqueuedAfter`; síncrono; devolve o `RedriveReport`.
- Respostas: `400` pedido inválido, `404` receiver inexistente, `409` execução em curso.
- Proteção (Spring Security) é da aplicação; o README traz um exemplo.

### Métrica

`inbox.redrive{consumer,source,target,result}` com `result` = `moved`, `skipped`, `error`. Sem o `redriveId`.

## Testes

1. `PassReaderTest` (core, InMemory): cada condição de fim. `DeadLetterPassTest` inalterado (prova da extração).
2. `InboxRedriveTest` (core, InMemory, síncrono): `FAILED → MAIN` (tentativas zeradas, `redrive=<id>`, sem
   `inbox_failure`, original confirmada); `FAILED → DEAD_LETTER`; `DEAD_LETTER → MAIN`; cada filtro; puladas vão ao
   fim e não encerram antes das que passam; `dryRun` (destino vazio, relatório agrupado); `limit`; `maxPerSecond`
   (5 mensagens a 10/s ≥ ~400 ms); falha ao copiar → `errors` e segue; pedidos inválidos; execução simultânea
   recusada.
3. Starter: `InboxAutoConfigurationTest` (bean e redrive ponta a ponta com o fake); `InboxRedriveEndpointTest`
   (desligado por padrão → 404; liga só com acesso + exposição; relatório; 400/404/409); `ObservabilityTest`
   (`inbox_redrive_total`).
4. `InboxContract`: `redriveFailedToMain` (mensagem em `failed` com `inbox_failure` volta à principal e é
   processada) e `redriveDryRunLeavesTheSource`, nos seis brokers; pulados no Pub/Sub (origem ilegível); na
   subscription do Service Bus, `FAILED → DEAD_LETTER` no lugar de `MAIN`.
5. `mvn clean install` verde.

## Documentação

- ADR-0012: revisão de 2026-11-07 — síncrono, `PassReader` compartilhado, MAIN recusado em tópico com subscription,
  Pub/Sub indisponível, `reason` por prefixo, respostas 400/404/409.
- README: seção de redrive (bean, endpoint, as duas chaves, exemplo de Spring Security, fluxo "inspeciona com
  `dryRun`, move pelo fingerprint").
- `messaging-inbox/docs/plan.md`: F5 feita.

## Riscos

- Execução síncrona longa (10.000 a 100/s ≈ 100 s) pode estourar timeout de proxy; o redrive segue até o fim no
  servidor e o log de auditoria registra o resultado.
- No SQS standard, cópias puladas podem voltar antes das que ainda não foram lidas e encerrar a execução mais cedo
  (`NEWER_THAN_PASS`); o operador repete.
- Uma execução por instância: duas instâncias podem redrivar a mesma origem ao mesmo tempo; leem com lease, então
  não movem a mesma mensagem duas vezes, mas o ritmo efetivo é a soma.
