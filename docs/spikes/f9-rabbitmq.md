# Spike F9: RabbitMQ por AMQP 0-9-1

- Data: 2026-10-06
- Broker: `rabbitmq:4.1-management-alpine` (4.1.8); cliente `com.rabbitmq:amqp-client` 5.35.0
- Origem: [ADR-0009](../adr/0009-activemq-e-rabbitmq.md)

| Pergunta | Resultado | Como |
|---|---|---|
| Contagem de entregas | Quorum queue: `x-delivery-count` **não vem** na primeira entrega e vale 1 depois do primeiro `basicNack(requeue)` (conta as entregas anteriores); fechar o channel também soma. Classic queue: só `redelivered` | Teste contra o broker |
| `message-id` no requeue | Mantido (o broker não gera; vem do produtor) | Teste |
| `mandatory` para fila inexistente | `basic.return` 312 `NO_ROUTE` chega **antes** do confirm, e o confirm é positivo | Teste |
| Exchange inexistente | O broker fecha o channel: 404 `NOT_FOUND` | Teste |
| Ack duplicado | Fecha o channel com 406 `PRECONDITION_FAILED - unknown delivery tag`, e as outras mensagens pendentes nele voltam à fila | Teste |
| Consumer ou `queueDeclarePassive` em fila inexistente | 404, e o channel fecha | Teste |
| Tamanho máximo | O `max_message_size` padrão do 4.x é 16 MiB. Acima disso, o broker fecha o channel com 406 "larger than configured max size" | Teste com 17 MiB |
| Consumer com prefetch e lease local | O `basicNack(requeue)` no fim do lease devolve a mensagem ao **mesmo** consumer, que ainda tem crédito. Um receiver parado continua segurando a mensagem no buffer, e outro receiver nunca a recebe | Contrato (`unackedMessageIsRedeliveredAfterLease`) |
| Netty | O `amqp-client` 5.35 declara Netty 4.2 (transporte opcional). No starter, ele se mistura ao Netty 4.1 do cliente do Artemis: `ClassNotFoundException: io.netty.util.concurrent.ThreadAwareExecutor`. Sem o Netty, o cliente usa o IO por socket padrão e o contrato passa | Teste do starter |

## Impacto

- **Receiver:** usa pull de verdade, com `basicGet`. Se a fila está vazia,
  tenta de novo a cada 100 ms até `maxWait`. Não usa `basicConsume` com
  prefetch, como o ADR previa. Sem consumer, um receiver parado não segura
  mensagens, e a mensagem devolvida no fim do lease fica para qualquer receiver.
  O custo é latência de até 100 ms e um RPC por tentativa.
- **`deliveryCount`:** é `x-delivery-count + 1` quando o header existe. Sem o
  header, vale 1 se `redelivered` for falso e fica vazio se for verdadeiro
  (classic queue).
- **Confirmação:** o adapter remove o tag da lista de pendentes antes de
  `basicAck`/`basicNack`. Assim nenhum tag é confirmado duas vezes.
- **Sender de fila:** usa `mandatory`, e o `NO_ROUTE` vira
  `DestinationNotFoundException`. O sender de exchange não usa `mandatory`:
  exchange sem fila ligada descarta a mensagem, como um tópico sem subscriptions.
- **Dependência:** o `messaging-rabbitmq` exclui `io.netty:*` do `amqp-client`.
