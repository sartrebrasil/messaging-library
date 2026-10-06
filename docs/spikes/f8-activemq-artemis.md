# Spike F8: ActiveMQ Artemis por Jakarta Messaging

- Data: 2026-10-06
- Broker: `apache/activemq-artemis:2.40.0`; cliente `org.apache.activemq:artemis-jakarta-client` 2.40.0
- Origem: pendências do [ADR-0009](../adr/0009-activemq-e-rabbitmq.md)

O cliente traz Netty 4.1. No repositório local faltam o `netty-transport-*-kqueue`
(transporte nativo do macOS) e o `commons-logging` 1.3.4. O parent exclui o
kqueue, que é dispensável porque o cliente usa NIO sem ele, e fixa o
`commons-logging` em 1.3.5.

| Pergunta | Resultado | Como |
|---|---|---|
| Fechar a session devolve a mensagem? | Sim. Mesmo `JMSMessageID`, `JMSXDeliveryCount` soma 1, `JMSRedelivered=true` | Teste contra o broker |
| Ordem do grupo depois de fechar a session | Mantida (`JMSXGroupID`) | Teste |
| `acknowledge()` depois de fechar a session | `jakarta.jms.IllegalStateException` "AMQ219019: Session is closed" | Teste |
| `MessageProducer.setDeliveryDelay` (JMS 2.0) | Funciona sem configuração no broker (3 s pedidos, entregue em 3,0 s) | Teste |
| `_AMQ_DUPL_ID` | O segundo envio com o mesmo id responde sucesso e é **descartado em silêncio** | Teste |
| Propriedades na mensagem recebida | `JMSXDeliveryCount`, os atributos e também `_AMQ_DUPL_ID`: as `_AMQ*` precisam ser filtradas | Teste |
| Existência sem criar | `ClientSession.queueQuery`/`addressQuery` (pela session Core do `ActiveMQSession`) não criam. Um `QueueBrowser` cria (auto-create) | Teste |
| Subscription de tópico | FQQN `<endereço>::<fila>`. Com auto-create, um consumer por FQQN antes do primeiro envio cria o endereço como **anycast**, e o envio ao tópico falha ("does not support MULTICAST routing"). A fila multicast precisa existir antes, como topologia | Teste |
| `consumerWindowSize` padrão | Com a janela de 1 MiB, o primeiro consumer bufferiza a segunda mensagem e outra session não a recebe. É obrigatório `consumerWindowSize=0` | Teste |
| Destino inexistente sem auto-create (`artemis create --no-autocreate`) | `Session.createQueue` lança `JMSException` genérico "There is no queue with name X", sem `InvalidDestinationException` | Contrato |
| Pinning de virtual thread no Java 21 | Sim: `ClientConsumerImpl.receive` faz `wait(toWait)` dentro de `synchronized (this)` | Leitura do código-fonte 2.40.0 |

## Impacto

- O `JmsDialect` entra com `ACTIVEMQ_CLASSIC` e `ARTEMIS`. No Artemis:
  - o atraso usa `deliveryDelay`, então `nack` com atraso não depende de configuração do broker;
  - o `checkAccess` usa `queueQuery` e `addressQuery`;
  - a subscription é lida por FQQN.
- O receiver recusa (`IllegalArgumentException`) uma conexão do Artemis com
  `consumerWindowSize` diferente de 0. O starter acrescenta `consumerWindowSize=0`
  à `broker-url`.
- O codec descarta as propriedades `_AMQ*` e `__AMQ*`.
- O `JmsErrors` trata "There is no queue with name" e as causas
  `*DoesNotExistException` como `DestinationNotFoundException`.
- Com `--no-autocreate`, o contrato `missingDestinationIsReported` roda no
  Artemis. No Classic ele continua pulado.
