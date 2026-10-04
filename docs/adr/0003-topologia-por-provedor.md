# ADR-0003: Topologia de fila, tópico e subscription por provedor

- Status: aceito
- Data: 2026-10-04

## Contexto

Os provedores organizam filas e tópicos de jeitos diferentes:

| | Fila | Tópico (envio) | Subscription (recebimento de tópico) |
|---|---|---|---|
| AWS | SQS | SNS | Fila SQS inscrita no tópico SNS |
| Azure | Service Bus Queue | Service Bus Topic | Service Bus Subscription |
| GCP | Não existe fila pura | Pub/Sub Topic | Pub/Sub Subscription |
| OCI | OCI Queue | OCI Notifications (ONS) | Não existe pull: o ONS só entrega por push (HTTPS, Functions, email, ...) |

Na AWS, sem raw message delivery, o SNS embrulha o corpo em um JSON e move os
atributos para dentro dele. No GCP, uma "fila" é um tópico com uma única
subscription.

## Decisão

Cada sender ou receiver é ligado a um único destino na construção:

| Provedor | Sender de fila | Sender de tópico | Receiver |
|---|---|---|---|
| AWS | URL da fila SQS | ARN do tópico SNS | URL da fila SQS (inscrita ou não no SNS) |
| Azure | Nome da fila | Nome do tópico | Nome da fila, ou tópico + subscription |
| GCP | Não se aplica: usa o tópico | `projects/{p}/topics/{t}` | `projects/{p}/subscriptions/{s}` |
| OCI | OCID da fila + endpoint de mensagens | Fora da v1 | OCID da fila + endpoint de mensagens |

- AWS: subscriptions SNS → SQS precisam de `RawMessageDelivery=true`. O adapter
  não consegue verificar isso sem permissão de leitura da subscription, então a
  regra é documentada e o contrato testa com raw delivery ligado. Mensagem
  recebida com corpo no formato de notificação SNS (`"Type":"Notification"`)
  gera um log de aviso, uma vez por receiver.
- OCI: fora da v1 (ADR-0001); quando entrar, só com Queue. O ONS fica fora porque não permite pull e porque a
  mensagem tem limite e formato próprios (título e corpo, sem atributos; a
  confirmar). Fan-out no OCI fica a cargo de quem publica, enviando para
  várias filas.
- O receiver não sabe se o destino é fila ou subscription. Para quem consome, é
  sempre "um lugar de onde vêm mensagens com lease".

## Consequências

- Trocar de fila para tópico, ou de provedor, muda configuração, não código.
- OCI fica assimétrico: sem tópico. A tabela de capabilities do README deixa isso
  visível.
- Na AWS, esquecer o raw delivery quebra atributos em produção sem erro claro.
  Por isso o aviso em log.
