# ADR-0004: Idempotência via idempotency-core

- Status: aceito (2026-10-08)
- Data: 2026-10-01
- Revisão: 2026-10-07 (chave pelo envelope, escopo compartilhado com o nível 3 e o redrive,
  `IN_PROGRESS` conta tentativa onde a contagem é do broker)
- Revisão: 2026-10-07 (`IN_PROGRESS` no nível 3; transação por chamada do handler em batch)
- Revisão: 2026-11-05 (store direto em vez do engine; owner token e dois TTLs feitos na idempotency-multi-store)

## Contexto

A idempotecy-multi-store tem um `IdempotencyEngine` e o SPI `IdempotencyStore`
(memory, JDBC, Redis, Mongo) independentes de Spring. O starter dela é voltado
a web/AOP. Lacunas que afetam mensageria:

- `complete`/`release` sem owner token: um consumer lento, com TTL expirado,
  pode apagar o registro de quem assumiu a mensagem depois. Conferido em
  2026-10-07: o `idempotency-core` ainda não tem owner token.
- JDBC grava em `REQUIRES_NEW`: crash entre o commit do negócio e o
  `complete` deixa a chave `IN_PROGRESS` até expirar.
- `IN_PROGRESS` vira exceção; num broker isso causa loop até a DLQ.
- Escopo padrão é `classe#método`: refatorar muda o escopo sem aviso.

A mesma mensagem lógica aparece com ids nativos diferentes: cópias de
`retry-queues`, DLQ, nível 3, `RESCHEDULE`/`SCHEDULED` da messaging-lib e
redrive ganham `messageId` novo (ADR-0002).

## Decisão

### Chave e escopo

- O middleware de idempotência chama o `IdempotencyEngine` direto, sem AOP.
- Chave = `InboxMessage.id`, que vem do envelope (`ce_id` ou `messageId` da
  outbox, ADR-0003) e é igual em todas as cópias. Em `envelope: raw` a chave é
  o id nativo e só protege contra reentrega da mesma mensagem física; o
  startup registra um aviso.
- Escopo = nome do consumer (explícito na configuração, nunca nome de classe).
  O nível 3 (`<consumer>.dead-letter`) e o redrive usam o **mesmo** escopo do
  consumer principal: o handler e os efeitos são os mesmos.

### Resultados

| Situação | Resultado |
|---|---|
| Chave nova | `reserve`, handler, `complete`, `ack` |
| Duplicata concluída | `Ack` sem chamar o handler (`replay = false`); `outcome=duplicate` |
| Mesma chave, payload diferente (fingerprint SHA-256 do payload) | `Failed`, `reason=idempotency_conflict` |
| `IN_PROGRESS` (outra instância processando) | `Retry(delay)` pela estratégia do nível 2; `outcome=in_progress`. Numa passada do nível 3, que não tem nível 2: volta para o fim da DLQ sem somar `l3` |
| Falha do handler | `release`; a próxima tentativa processa de novo |
| Cópia para DLQ ou `failed` | Envia a cópia, `release`, `ack`. Libera a chave para o nível 3 e o redrive |

`IN_PROGRESS` e a contagem de tentativas: não conta onde a inbox controla a
contagem (`hold`, `retry-queues`, reenfileiramento no fim da DLQ). Em
`broker-delay` conta, porque o `deliveryCount` do broker cresce a cada `nack`
e a inbox não tem como descontar. A versão anterior deste ADR dizia "sem
contar tentativa" em todos os casos, o que não é possível com `broker-delay`.

### Ordem das operações

```
sucesso: reserve -> handler -> complete -> ack
falha:   reserve -> handler (falha) -> release -> efeito do Outcome
DLQ:     reserve -> handler (falha) -> envio da cópia -> release -> ack
```

- `complete` antes do `ack`: se o `ack` falhar (lease vencido), a reentrega
  vira `duplicate`.
- Se o `release` falhar, a chave fica `IN_PROGRESS` até o TTL de reserva
  vencer. Nesse meio tempo, a mensagem que voltar vira `in_progress`.

### TTLs

| TTL | Regra |
|---|---|
| Reserva (`IN_PROGRESS`) | Maior que o tempo máximo de processamento de uma entrega: `max-processing` (ADR-0009) mais a soma dos atrasos do `hold`, quando for a estratégia. Validado no startup. O `reserve` é feito na vez da mensagem, então a espera atrás de outras mensagens do mesmo `receive` não conta. No batch, o `reserve` vale para a chamada do lote inteiro, e `max-processing` é o tempo do lote |
| Registro concluído (`idempotency.ttl`, padrão `P7D`) | Precisa cobrir a janela em que uma duplicata pode chegar: reentregas, nível 3 e redrive. Redrive de uma mensagem concluída depois do TTL processa de novo |

### Transação

- Com store JDBC e o mesmo `DataSource` do negócio, a reserva e o `complete`
  entram na transação do handler (padrão inbox transacional): ou tudo
  confirma, ou nada.
- Handler em batch: uma transação por chamada do handler (o `bisect` faz várias
  chamadas por lote), com `complete` só das mensagens que deram certo
  (ADR-0011).
- Redis e Mongo são at-least-once com janela documentada.

### Pré-requisitos na idempotecy-multi-store (fase F1)

1. Owner token em `reserve`/`complete`/`release` (Redis com script Lua
   atômico; JDBC com `WHERE owner = ?`).
2. Modo de transação do JDBC configurável: `join` ou `requires-new`.
3. `spring-web` opcional no starter, para a inbox reaproveitar os beans de
   store sem puxar dependências web.
4. Migração para Spring Boot 4 e Jackson 3.

## Revisão de 2026-11-05: implementação

- O middleware usa o `IdempotencyStore` direto, e não o `IdempotencyEngine`.
  O engine conclui a chave sempre que a operação retorna e não deixa a inbox
  liberá-la em `RetryAfter`, `Reject` e falhas, nem manter uma reserva só
  durante o nível 1 inteiro.
- Pré-requisitos feitos na idempotency-multi-store (branch `feat/owner-token`, integrada na `main` em 2026-11-05):
  owner token em `complete`/`release` nos quatro stores (JDBC com
  `WHERE owner = ?`, Redis com script Lua, Mongo com filtro); `complete` aceita
  uma nova expiração, o que dá os dois TTLs da tabela acima (reserva curta,
  concluído com `idempotency.ttl`); modo `join` no JDBC, com o `reserve` num
  savepoint. Fica para a fatia 9: `spring-web` opcional no starter.
- `release` acontece antes do efeito no broker, inclusive antes da cópia para
  DLQ ou `failed` (a versão acima dizia depois da cópia). Um crash entre o
  `release` e a cópia só reprocessa uma mensagem que já tinha falhado; um crash
  entre a cópia e o `release` deixaria a chave presa até o TTL da reserva.
- Falha do store (no `reserve` ou no `complete`) é uma falha retryable que
  pula o nível 1: vai para o nível 2 e depois para a DLQ. Sem `complete` não há
  `ack`; a reserva é liberada.
- `IN_PROGRESS` com o nível 2 já esgotado (só acontece em `broker-delay`, onde
  cada reentrega conta) vai para a DLQ (ou `failed` sem DLQ).
- O `idempotency-core` é dependência opcional do `messaging-inbox`; a lógica
  fica numa classe à parte, carregada só pelos consumers com idempotência.

## Consequências

- Idempotência não é obrigatória: sem store configurado, o middleware não
  entra no pipeline. Sem ela, cópias e redrive podem processar duas vezes.
- Métricas de duplicata ficam na inbox, não na lib de idempotência.
- Produtores sem envelope (`raw`) não têm idempotência entre cópias. O README
  recomenda CloudEvents binário.
