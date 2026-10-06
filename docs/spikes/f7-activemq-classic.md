# Spike F7: ActiveMQ Classic por Jakarta Messaging

- Data: 2026-10-06
- Broker: `apache/activemq-classic:6.2.0`; cliente `org.apache.activemq:activemq-client` 6.2.5
- Origem: pendências do [ADR-0009](../adr/0009-activemq-e-rabbitmq.md)

No Classic 6.x, o `activemq-client` já é Jakarta. O `activemq-client-jakarta`
só existe na linha 5.18.

| Pergunta | Resultado | Como |
|---|---|---|
| `MessageProducer.setDeliveryDelay` (JMS 2.0) | **Não suportado**: `UnsupportedOperationException`, com ou sem `schedulerSupport` | Teste contra o broker |
| `AMQ_SCHEDULED_DELAY` | Funciona com `schedulerSupport="true"` (entregue em cerca de 3,1 s para 3 s). **Sem o scheduler, a propriedade é ignorada em silêncio** e a entrega é imediata | Teste contra os dois brokers |
| Fechar a session devolve a mensagem? | Sim. Mesmo `JMSMessageID`, `JMSXDeliveryCount` soma 1, `JMSRedelivered=true`, e outra session recebe | Teste |
| Fechar só o consumer | Também devolve e soma 1 | Teste. Não é usado: fechar a session é o caminho mais simples |
| Ordem do grupo depois de fechar a session | Mantida: `m1` volta antes de `m2` e `m3` (`JMSXGroupID`) | Teste |
| Duas sessions `CLIENT_ACKNOWLEDGE`, uma mensagem em cada | O ack de uma não afeta a outra; fechar uma devolve só a sua | Teste |
| `acknowledge()` depois de fechar a session | `jakarta.jms.IllegalStateException` "The Consumer is closed" | Teste |
| `JMSXDeliveryCount` na primeira entrega | 1 | Teste |
| `QueueBrowser` em fila inexistente | **Cria a fila** (auto-create). O `DestinationSource` da conexão lista as filas por advisory, sem criar, mas chega de forma assíncrona (cerca de 1 s) | Teste |
| Pinning de virtual thread no Java 21 | Sim: `FifoMessageDispatchChannel.dequeue` faz `mutex.wait(timeout)` dentro de `synchronized`. No Java 24+ (JEP 491) não prende | Leitura do código-fonte 6.2.5 (a JVM local é 24) |

## Impacto

- O `nack` com atraso usa `AMQ_SCHEDULED_DELAY` e exige `schedulerSupport` no
  broker. O adapter não consegue detectar a falta dele. Por isso o atraso é
  opt-in (`Redelivery.SCHEDULED`), e o padrão (`IMMEDIATE`) ignora o atraso.
- `orderedDelivery` é `true` em `IMMEDIATE`, porque toda reentrega volta para a
  frente do grupo. Em `SCHEDULED`, a cópia vai para o fim da fila, então é
  `false`.
- O `checkAccess` usa o `DestinationSource` e exige uma `ActiveMQConnection`
  (sem pool por cima). Destino inexistente custa 2 s de espera.
- O contrato `missingDestinationIsReported` não se aplica: com auto-create, envio
  e receive para destino inexistente não falham.
