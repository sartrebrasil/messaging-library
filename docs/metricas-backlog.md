# Backlog de métricas

Saiu da revisão de 2026-11-13 das métricas do starter (`MicrometerConsumerObserver`, `MicrometerSupport`,
`MeteredInboxRedrive`, `ObservedMessageSender`, `MeteredMessageReceiver`) contra o ADR-0007 da inbox e o ADR-0008 da
messaging-lib. A ordem sugerida é: grupo 1 (nomes) primeiro, porque renomear depois das métricas novas dobra o
trabalho; em seguida grupo 2 (correções), grupo 3 (métricas novas) e grupo 4 (documentação).

Janela para renomear: o engine-tax, único consumidor conhecido, ainda não fez merge nem implantou a inbox
(veja Pendências em `messaging-inbox/docs/plan.md`). Depois da implantação, renomear exige período de convivência
dos dois nomes.

## 1. Padronização dos nomes

- [x] **N1. Decidir e registrar a convenção** (revisão do ADR-0007 da inbox e do ADR-0008 da messaging-lib).
  Proposta:
  - prefixo `inbox.` para semântica de consumo e `messaging.` para chamadas ao broker;
  - counter: `<prefixo>.<entidade no plural>.<evento no particípio>` (`inbox.messages.received`);
  - timer: sufixo `.duration` sempre; idade: sufixo `.age`;
  - tags e valores em `snake_case`, minúsculos;
  - todo meter com `description` e, em timer/summary/gauge, `baseUnit`;
  - mesmo padrão da outbox-library (`outbox_*`), para os dashboards das duas libs lerem igual.
- [x] **N2. Chaves de tag das observations** (feito em 2026-11-13): opção (a), chaves curtas nos timers. O
  desenho que fez isso caber sem perder o padrão OpenTelemetry no span: no Micrometer, as chaves de baixa
  cardinalidade viram tags do timer, e as de alta cardinalidade só vão para o span. Então `consumer`, `destination`,
  `level` e `outcome` (e `provider` + `destination` no `messaging.publish`) ficaram em baixa cardinalidade, e os nomes
  OTel (`messaging.system`, `messaging.destination.name`...) foram para alta. Sem `ObservationConvention`. O span ganha
  atributos duplicados (`consumer` e `messaging.consumer.group.name`, por exemplo) e perde `inbox.level` e
  `inbox.outcome`, que viraram `level` e `outcome`. Não resolvido: a tag `error`, fixa no handler do Micrometer, segue
  como `error` (as dos meters explícitos é `exception`); só trocando o `DefaultMeterObservationHandler` por um nosso.
- [x] **N3. Renomear os meters da inbox** conforme N1. Proposta:

  | Hoje | Proposto |
  |---|---|
  | `inbox.messages.received` | sem mudança |
  | `inbox.messages.processed` | sem mudança |
  | `inbox.retry` | `inbox.messages.retried` |
  | `inbox.dead.letter` | `inbox.messages.dead.lettered` |
  | `inbox.failed` | `inbox.messages.failed` |
  | `inbox.redrive` | `inbox.messages.redriven` |
  | `inbox.retry.skipped` | `inbox.retries.skipped` |
  | `inbox.effect.failed` | `inbox.effects.failed` |
  | `inbox.attribute.budget.exceeded` | `inbox.attributes.budget.exceeded` |
  | `inbox.dead.letter.passes` | sem mudança |
  | `inbox.processing.duration` | sem mudança |
  | `inbox.message.age` | sem mudança |
  | `inbox.last.receive.age` | sem mudança |

  Feito em 2026-11-13: código, testes, README e ADRs 0002, 0007, 0009 e 0012. `inbox_redrive` como nome de
  atributo de mensagem (ADR-0002) não é métrica e ficou como estava.
- [x] **N4. Renomear os timers do transporte** para ter `.duration`: `messaging.send` → `messaging.send.duration`,
  `messaging.receive` → `messaging.receive.duration`, idem `ack`, `nack` e `dead.letter`. Alternativa a avaliar junto
  com N2: um timer só, `messaging.operation.duration{operation=send|receive|ack|nack|dead_letter}`, que é o desenho da
  convenção OTel (`messaging.client.operation.duration`) e simplifica os painéis. Feito só o sufixo
  `.duration` (README e ADR-0008 da lib atualizados); o timer único fica como opção para N2. `baseUnit` em
  `messaging.receive.messages` não foi feito: confirmar antes que o Prometheus não duplica o sufixo.
- [x] **N5. Manter a tag `destination` na inbox (decidido em 2026-11-13).** Ela é a chave que une as métricas da
  inbox às do transporte: o receiver é criado com `c.name()` (`ConsumerBuilder.java:81` e `:130`), e o
  `MeteredMessageReceiver` grava esse mesmo nome em `destination`; a passada do nível 3 casa por `<nome>.dead-letter`.
  Vale o nome lógico do receiver, nunca o endereço (`ConsumerBuilder.java:109`). Para fixar o contrato: N9 passa a
  conferir que `inbox_*` e `messaging_*` do mesmo receiver têm o mesmo valor de `destination`, e o ADR-0007 registra a
  regra. `consumer` e `destination` seguem iguais hoje; se um dia o receiver puder ler de um destino lógico
  diferente do seu nome, é aqui que eles divergem.
- [x] **N6. Padronizar `messaging.system`** (feito em 2026-11-13): o span da inbox usa o tipo do provider, o mesmo
  valor da tag `provider` do transporte (`MessagingProperties.Type.tag()`, que o `MessagingDestinations` também usa).
  Decisão: não usar os valores do OpenTelemetry, porque `aws` cobre SQS e SNS e `aws_sqs` seria errado para o SNS.
  `messaging` quando o tipo não é conhecido (um `InboxDestinations` próprio, sem `MessagingProperties`).
- [x] **N7. Passada do nível 3 sem consumer sufixado** (feito): `consumer="<nome>"` nas duas, `destination` com
  `<nome>.dead-letter`, que é o nome do receiver da DLQ no transporte, então o cruzamento do N5 continua valendo.
  Não precisou de tag `level`: o C2 já resolveu a contagem dupla, e o `destination` separa as séries. Consequência:
  `inbox.effects.failed`, `inbox.retries.skipped` e `inbox.attributes.budget.exceeded`, que só tinham `consumer`,
  ganharam `destination` (senão a passada e o loop principal cairiam na mesma série).
- [x] **N8. Padronizar a tag de erro** (feito): `exception` nos timers de `messaging.*` (nome simples da classe
  lançada, ou `none`); `outcome` fica para o resultado. Não usei a causa raiz como na inbox: o adapter já traduz o
  erro do SDK para um `MessagingException` específico (throttled, acesso negado...), e esse é o tipo útil.
- [x] **N9. Teste-guarda dos nomes** (feito em 2026-11-13): `MeterNamesTest` em `inbox.spring` e em `spring`. Cada um
  aciona os meters de verdade num `SimpleMeterRegistry` e compara nome, tipo e chaves de tag com uma lista fixa,
  e exige `description` em todos. Mudar um nome exige mudar a lista e o ADR-0007 juntos. Não cobre:
  - `inbox.last.receive.age` (gauge): precisa do consumer inteiro; segue no `ObservabilityTest`;
  - os timers que o `DefaultMeterObservationHandler` gera de `inbox.consume` e `messaging.publish` (M11);
  - a igualdade de `destination` entre `inbox_*` e `messaging_*`: cada teste confere só o seu lado (o nome do
    receiver). Que o `ConsumerBuilder` crie o receiver com `c.name()` está no código (`:81`, `:130`), sem teste.
  - unidade (`baseUnit`): os timers saem em segundos pelo Prometheus e o gauge declara `seconds`; não conferida.

## 2. Correções nas métricas atuais

- [x] **C1. Lease perdida no `ack` sai como sucesso** (feito em 2026-11-13): `outcome="lease_expired"`, documentado
  no ADR-0007. Teste: `OutcomeTagsTest`.
- [x] **C2. Falha na passada do nível 3 conta como entrada na DLQ** (feito): o observer da passada
  (`pass=true`) emite `outcome="retry_dead_letter"` e `inbox.messages.retried{level="dead_letter", strategy="none"}`,
  e não incrementa `inbox.messages.dead.lettered`. Isso muda a série: a DLQ deixa de contar as voltas da passada.
  Não foi preciso mexer em N7 (o nome `<consumer>.dead-letter` continua).
- [x] **C3. `effect` deduzido em `inbox.effects.failed`** (feito): `Applied` e `ConsumerObserver.Report` ganharam
  `failedCall` (`ACK`, `NACK`, `COPY`), preenchido onde a chamada falha. Corrigiu também o caso que a descrição
  original não via: `ack` que falha depois de uma cópia bem-sucedida saía como `copy`. Mudou a API pública
  `Report` (um componente a mais).
- [x] **C4. Lote medido como envio unitário** (feito): tag `batch=true|false` em `messaging.send.duration` e
  `messaging.ack.duration`. As outras operações não têm forma em lote e ficaram sem a tag.
- [x] **C5. Falha parcial de lote sai como sucesso** (feito só o que cabia aqui): `outcome="partial"` quando parte
  dos itens falha e `error` quando todos falham, em `sendAll` e `ackAll`. A contagem por item e por exceção
  continua no M5.
- [x] **C6. Redrive: o que valia medir** (reinterpretado): o redrive só lança exceção em requisição recusada
  (receiver desconhecido, já em execução, configuração), e contar isso poria o `consumer` do chamador numa tag, sem
  limite de cardinalidade. Falha operacional volta no relatório (`end=receive_failed`) e não aparecia. Novo
  `inbox.redrives.ended{consumer,source,target,end}`, com o `end` do relatório. Cobre a parte de contagem do M8;
  falta só a duração (`inbox.redrive.duration`).

## 3. Métricas novas

Prioridade alta:

Feitos em 2026-11-13. Os nomes seguem a convenção do ADR-0007 e por isso diferem das propostas originais.

- [x] **M1. `inbox.keepalives.failed{consumer,destination}`:** `keepAlive` que não pôde começar. Novo gancho
  `ConsumerObserver.keepAliveFailed`, chamado no `Worker` e na `DeadLetterPass`. Não cobre o lease que se perde
  *depois* de renovar (o `KeepAlive.lost()` ninguém lê hoje).
- [x] **M2. `inbox.failures.unexpected{consumer,destination,stage,exception}`:** `stage` = `lane`, `delivery`,
  `dead_letter_pass`. Gancho `unexpectedFailure`.
- [x] **M3. `inbox.messages.in.flight{consumer,destination}` (gauge):** `received` incrementa, `delivered`
  decrementa. Vale porque o `Worker` sempre chama `delivered` depois de `received` (até parando: libera e reporta);
  só um `OutOfMemoryError` deixaria vazar. O consumer é recriado a cada start, então o gauge do anterior é
  removido e registrado de novo (teste: `aRestartedConsumerReplacesTheGaugeOfTheOneBefore`).
- [x] **M4. Lease vencida:** coberto pelo C1 (`outcome="lease_expired"`).
- [x] **M5. `messaging.messages.failed{provider,destination,operation,exception}`:** um counter só, com
  `operation` = `send`, `ack`, `nack`, `dead_letter`, em vez de dois como o backlog propunha. Conta por item em
  lote parcial, e todas as mensagens da chamada quando ela lança.
- [x] **M6. `inbox.handler.duration{consumer,destination,outcome}`:** uma chamada do handler, cada retry imediato
  é uma. `outcome` = `success` ou `error` (lançou); `Reject` e `Discard` contam como `success`. Gancho `handled`.
- [x] **M7. `inbox.idempotency.duration{consumer,destination,operation,outcome}`:** `reserve`, `complete`,
  `release`. Gancho `idempotency`; o `IdempotencyGuard` passou a receber o observer.
- [x] **M8. `inbox.redrive.duration{consumer,source,target}`:** a contagem das execuções saiu no C6
  (`inbox.redrives.ended`). Pedido recusado não é medido (o `consumer` seria entrada do chamador).
- [x] **M9. `inbox.delivery.attempts{consumer,destination,level}`:** distribution summary de
  `delivery().attempt()`, uma vez por entrega com envelope lido.
- [x] **M10. `inbox.hold.wait.duration{consumer,destination}`:** a espera *planejada* (o `delay` do retry), não a
  medida; as duas só diferem se a espera for interrompida.
- [x] **M11. Timers das observations** (feito com o N2): documentados no ADR-0007 (`inbox_consume_seconds`,
  `inbox_consume_active_seconds`) e no README (`messaging_publish_*`), e travados por `ObservationMetersTest` e
  `MeterNamesTest`. Dois achados da sonda com o handler de verdade: o `.active` tem `outcome` sempre `released`
  (só se sabe no fim), e `inbox_consume_seconds` conta execuções, não entregas.

Bloqueados:

- [ ] **M12–M14** (`inbox.circuit.*`, `inbox.batch.size`, `inbox.stale.races`, outcomes `stale` e `expired`):
  dependem das features (F6 pausado, ADR-0011, ADR-0013, ADR-0015), que não existem no código.
- [x] **M15. `messaging.message.size{provider,destination}`:** distribution summary em bytes, uma vez por
  mensagem enviada (inclusive em `sendAll`), medida antes do envio, então conta também o que falhou.
- [ ] **M16. Profundidade da fila e da DLQ:** não feito. Pede uma capability nova no core e uma implementação por
  adapter (SQS, Service Bus, Pub/Sub, JMS, RabbitMQ), e é uma decisão de design à parte.

## 4. Documentação

Feitos em 2026-11-13.

- [x] **D1. Tabela de métricas do ADR-0007** com os nomes exportados de verdade (`_seconds` nos timers,
  `inbox_dead_letter_passes_total`, nomes finais do N3). Conferido por script: os 22 nomes derivados do que os
  guards registram aparecem no README e no ADR-0007 da inbox; os do transporte, no README raiz e no ADR-0008.
- [x] **D2. Valores de `end`** corrigidos para os de `PassEnd` (`empty`, `newer_than_pass`, `repeated_key`,
  `max_per_pass`, `stopping`, `receive_failed`).
- [x] **D3. O que ainda não existe** marcado como **planejado** no ADR-0007: `inbox_circuit_*`, `inbox_batch_size`,
  `inbox_stale_race_total`, outcomes `stale`, `expired` e `circuit_open`, reason `expired` e o span por batch.
  Também corrigi a frase de abertura da Decisão, que dizia que a observation gerava todas as métricas.
- [x] **D4. README da inbox e ADR-0008 da messaging-lib** com a lista final, incluindo os timers das observations
  (M11), exemplos de PromQL novos (em andamento, handler, `lease_expired`, falhas inesperadas, redrive) e alertas
  sugeridos para as métricas novas. Os exemplos de PromQL não foram executados contra um Prometheus: só os nomes
  foram conferidos contra o código.
