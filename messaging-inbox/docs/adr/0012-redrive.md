# ADR-0012: Ferramenta de redrive

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-10-07 (fingerprint, limite de taxa e `inbox_redrive`; vindos da acme-messaging)
- Revisão: 2026-10-07 (mensagens puladas voltam para o fim da origem; marca de redrive em
  `inbox_attempts`)
- Revisão: 2026-11-07 (implementação: síncrono, `PassReader` compartilhado com o nível 3, `end` no relatório,
  destino principal recusado em tópico com subscription, origem ilegível indisponível, `reason` por prefixo,
  respostas 400/404/409)

## Contexto

Depois de corrigir um bug ou uma configuração, as mensagens da `failed` (e às
vezes da DLQ) precisam voltar para a fila principal. Os mecanismos nativos não
servem para todos os provedores: o redrive da AWS só existe no SQS, o Service
Bus exige ferramenta externa, e RabbitMQ e ActiveMQ dependem de shovel ou
console. Além disso, um redrive nativo não sabe zerar os atributos `inbox_*`
(ADR-0002), e a mensagem voltaria com as tentativas já esgotadas.

## Decisão

### Operação

```java
public interface InboxRedrive {
    RedriveReport redrive(RedriveRequest request);
}

public record RedriveRequest(String consumer,
                             Source source,          // DEAD_LETTER | FAILED
                             Target target,          // MAIN | DEAD_LETTER
                             int limit,              // obrigatório, máx. 10_000
                             Filter filter,          // exceção, fingerprint, reason, enfileirada antes/depois de
                             int maxPerSecond,       // padrão 100
                             boolean dryRun) {}

public record RedriveReport(String redriveId, PassEnd end, int moved, int skipped, int errors,
                            Map<String, Integer> byException,
                            Map<String, Integer> byFingerprint) {}
```

- `byFingerprint` usa o fingerprint do ADR-0017, lido do `inbox_failure`.
  Inspecionar a DLQ ou a `failed` é um `dryRun` sem filtro: o relatório mostra
  os grupos, e o redrive seguinte filtra pelo fingerprint do bug corrigido.
- `maxPerSecond` limita as cópias enviadas ao destino, para o redrive de 40
  mil mensagens (quatro execuções de 10.000) não derrubar a dependência que
  acabou de voltar. As mensagens puladas não contam.

| Origem → destino | Uso |
|---|---|
| `failed` → principal | Bug corrigido; reprocessar do zero |
| `failed` → DLQ | Reentrar só no nível 3 |
| DLQ → principal | Antecipar a passada do nível 3 com tentativas zeradas |

### Como move

Cada execução começa com `runStart = now`. Para cada mensagem lida da origem,
até `limit`:

1. Não passa no filtro (`skipped`): cópia **para o fim da própria origem**,
   sem mudar nada, depois `ack`. É o mesmo movimento do reenfileiramento do
   nível 3 (ADR-0002). Com `nack(0)`, a mensagem pulada voltaria na hora, seria
   lida de novo como "já vista" e encerraria a execução antes de chegar às
   mensagens que passam no filtro. `nack(atraso)` também não serve: Service
   Bus `ABANDON`, ActiveMQ `IMMEDIATE` e RabbitMQ não têm atraso.
2. Passa (`moved`): envia a cópia para o destino e depois faz `ack` na origem.
   Mesma ordem do ADR-0002: duplicata, nunca perda.
3. A cópia para o destino leva corpo, `contentType`, `orderingKey`,
   `traceparent` e os atributos do usuário. Sai sem `inbox_failure` e sem
   `dead_letter`, e com `inbox_attempts` = `l2=0;l3=0;redrive=<redriveId>`: as
   tentativas recomeçam do zero. A marca de redrive acompanha a mensagem em
   todas as cópias seguintes (ADR-0002), aparece no log do consumer e faz a
   mensagem não expirar (ADR-0015). `deduplicationId` = `messageId` de origem.
4. Para quando chega uma mensagem com `enqueuedAt >= runStart` (uma pulada
   desta execução) ou cuja chave de idempotência já apareceu nesta execução
   (proteção para provedor sem `enqueuedAt`), quando a origem volta vazia ou
   quando chega a `limit`. Mesmas condições do fim de passada do nível 3.

- `dryRun`: lê, aplica o filtro e monta o relatório, e trata **todas** as
  mensagens como puladas (cópia para o fim da origem). Nada vai para o
  destino.
- A origem muda de ordem: as puladas vão para o fim. Na DLQ e na `failed`, a
  ordem nunca valeu (ADR-0005).
- Um redrive por consumer e origem de cada vez, por instância. Uma passada do
  nível 3 rodando na DLQ ao mesmo tempo não atrapalha: as duas leem com lease.
- O redrive não chama o handler. O processamento é o do consumer normal, com
  idempotência: uma mensagem que já tinha sido concluída antes de ir para a
  `failed` vira `duplicate`, desde que o `idempotency.ttl` não tenha vencido
  (ADR-0004).

### Interfaces

| Interface | Detalhe |
|---|---|
| API Java | Bean `InboxRedrive` no starter |
| Actuator | `POST /actuator/inboxredrive/{consumer}` com o `RedriveRequest` em JSON; `GET` lista os receivers com as origens (disponível, ou o motivo) e os destinos |

- O endpoint é de escrita e vem **desligado**. Para ligar, é preciso
  `management.endpoint.inboxredrive.access=unrestricted` e expor o endpoint
  explicitamente. A proteção (Spring Security) é da aplicação.
- Cada execução gera um `redriveId` (UUID) e um log de auditoria com o id,
  consumer, origem, destino, filtro e contagens, sem payload. Métrica
  `inbox_redrive_total` (ADR-0007), sem o id.

### Implementação (revisão de 2026-11-07)

- Síncrono: a chamada (e o `POST`) volta quando a execução termina. 10.000 mensagens a 100/s levam cerca de 100 s;
  se um proxy cortar a resposta, a execução segue no servidor e o log de auditoria registra o resultado.
- O laço de leitura é o `PassReader`, o mesmo do nível 3 (ADR-0002): as condições de fim são as mesmas, e o relatório
  diz qual encerrou a execução (`end`: `empty`, `newer_than_pass`, `repeated_key`, `max_per_pass`,
  `receive_failed`).
- Filtro: `exception` e `fingerprint` iguais aos do `inbox_failure` (`none` sem o atributo); `reason` é prefixo do
  motivo do `dead_letter` (`rejected`, `java.lang.IllegalArgumentException`); `enqueuedBefore`/`enqueuedAfter` sobre
  `enqueuedAt`, e mensagem sem `enqueuedAt` não passa num filtro de data.
- `maxPerSecond` (o core exige ≥ 1; o endpoint usa 100 quando omitido): a k-ésima movida sai depois de
  `(k-1)/maxPerSecond` do início. `limit` (1..10.000) conta as lidas, movidas ou não.
- Destino principal só quando o receiver é uma fila. Num tópico com subscription, enviar ao tópico chegaria a todas
  as subscriptions: o pedido é recusado, e o caminho é `failed` → DLQ (o nível 3 reprocessa).
- No Pub/Sub, `failed` e a DLQ são tópicos e não podem ser lidos: a origem fica indisponível, com o motivo no `GET`.
- Erros: pedido inválido `IllegalArgumentException` (400), receiver inexistente `NoSuchElementException` (404),
  execução em curso no mesmo receiver e origem `IllegalStateException` (409).
- Uma execução por instância: duas instâncias podem redrivar a mesma origem ao mesmo tempo. Leem com lease, então não
  movem a mesma mensagem duas vezes, mas o ritmo efetivo é a soma.
- No SQS standard, sem ordem, uma pulada pode voltar antes das que ainda não foram lidas e encerrar a execução mais
  cedo (`newer_than_pass`); o operador repete.

### Ordem

O redrive nunca preserva ordem: a mensagem volta depois de versões mais novas
do mesmo aggregate. O stale check (ADR-0013) é o que impede aplicar uma versão
velha.

### Kafka

A origem é um tópico (o `dead-letter` ou o `failed` do consumer, ADR-0008)
lido por um consumer group próprio, `<consumer>.redrive`. O `ack` é o commit do
offset. Num log, pular é só não copiar: a mensagem continua no tópico, e não
há cópia para o fim. No `dryRun` o offset não é comitado.

## Alternativas descartadas

- Redrive nativo do SQS (`StartMessageMoveTask`): só AWS e não limpa os
  atributos `inbox_*`.
- CLI própria: exige empacotar e distribuir um binário, com acesso às mesmas
  credenciais da aplicação. Fica para depois, se o endpoint não bastar.
- Peek sem consumir: a messaging-lib não expõe peek (só o Service Bus tem), e o
  `dryRun` com cópia para o fim da origem cobre o caso.
- Puladas com `nack(0)` ou `nack(atraso)` (versão anterior deste ADR): ver
  "Como move".
- Hook de transformação do payload no redrive (acme-messaging): a mensagem
  corrigida teria o mesmo id com payload diferente e cairia em
  `idempotency_conflict` (ADR-0004). Corrigir dado é do produtor.
- `archive` para storage externo (acme-messaging): fica para o claim-check com
  a storage-lib (F8+).

## Consequências

- Mesmo procedimento de redrive em todos os provedores.
- O endpoint é uma superfície de escrita na aplicação. Fica desligado por
  padrão, e o README mostra como protegê-lo.
