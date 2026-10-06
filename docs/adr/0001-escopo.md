# ADR-0001: Escopo: data plane de filas e tópicos de nuvem, sobre os SDKs oficiais

- Status: aceito
- Data: 2026-10-04

## Contexto

Os serviços da equipe usam o SDK da AWS direto para SQS e SNS. Trocar de nuvem
hoje significa reescrever todo código de envio e consumo. A storage-lib já
resolveu o mesmo problema para object storage com um core sem dependências e um
adapter por provedor.

Para mensageria, as opções existentes trazem peso que não queremos:

| Opção | Prós | Contras |
|---|---|---|
| Spring Cloud Stream | Binders para vários brokers | Prende ao Spring e ao modelo de binder; retry e DLQ diferentes por binder; binder de SQS só da comunidade |
| Spring Cloud AWS/Azure/GCP/OCI | Integração Spring de cada nuvem | Uma API por nuvem: não abstrai nada; versões atrás do Spring Boot |
| Apache Camel | Componentes para todas as nuvens | Framework inteiro, DSL própria |
| Dapr pub/sub | Abstração pronta | Exige sidecar e operação de Dapr |
| JMS | API padrão | Só Service Bus e IBM MQ têm cliente JMS de verdade; SQS com limitações; Pub/Sub e OCI sem cliente |
| **SDKs oficiais atrás de uma API própria** | Zero dependência além do SDK; mesma receita da storage-lib | Escrever e manter um adapter por provedor |

## Decisão

Dentro do escopo:

- Envio para fila e tópico; recebimento de fila e subscription.
- Confirmação, devolução com atraso, extensão de lease e dead letter (ADR-0005).
- Provedores da v1: AWS (SQS, SNS), Azure (Service Bus), GCP (Pub/Sub). Os três
  têm emulador, então o contrato roda no CI, e três modelos diferentes (fila +
  tópico separados, entidade única, só tópico) validam a abstração.
- OCI (Queue) entra depois da v1: não tem emulador, não tem tópico com pull
  (ADR-0003) e o contrato só rodaria manualmente. A API não pode depender de
  nada que o OCI Queue não tenha; os ADRs mantêm a coluna do OCI por isso.
- `messaging-core` só com JDK; cada adapter traz só o SDK oficial do seu
  provedor (`software.amazon.awssdk`, `com.azure:azure-messaging-servicebus`,
  `com.google.cloud:google-cloud-pubsub`, `com.oracle.oci.sdk:oci-java-sdk-queue`).

Fora do escopo:

- Criar, alterar ou apagar filas, tópicos, subscriptions, DLQs e permissões. Isso
  é Terraform, como bucket na storage-lib.
- Kafka, Kinesis, Event Hubs, OCI Streaming: são logs com offset, não filas com
  lease (ADR-0002). A inbox-library mantém o adapter Kafka próprio.
- RabbitMQ, ActiveMQ e IBM MQ: não são serviços de nuvem gerenciados com SDK
  próprio. Podem entrar depois se a API aguentar, sem compromisso. ActiveMQ e
  RabbitMQ entraram depois da v1 (ADR-0009).
- Listener, polling em loop, concorrência, retry, idempotência, serialização.
  São responsabilidades de quem consome (a inbox-library, por exemplo).
- Claim-check para payload grande: combinação da storage-lib com esta, feita por
  quem usa.
- API reativa.

## Consequências

- A lib é pequena e previsível: cada método vira uma ou poucas chamadas do SDK.
- Trocar de provedor muda dependência e configuração, não código. A semântica
  que não dá para igualar fica explícita nas capabilities (ADR-0007), não
  escondida.
- Upgrades seguem o SDK de cada provedor, não o ciclo dos projetos Spring Cloud.
