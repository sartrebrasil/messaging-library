# ADR-0011: Clientes nativos fornecidos pela aplicação

- Status: aceito
- Data: 2026-11-15
- Substitui: a seção "1. Uma regra única para o bean da aplicação" do [ADR-0010](0010-configuracao-tipada-dos-providers.md)
- Relacionada: [ADR-0001](0001-escopo.md), [ADR-0008](0008-spring-boot-e-observabilidade.md)

## Contexto

O ADR-0010 deixou o starter com dois caminhos para o cliente nativo de cada provider: ele cria o cliente a partir
de `messaging.providers.<nome>` (`region`, `endpoint`, `access-key`, `secret-key`, `connection-string`,
`emulator-host`, `broker-url`, `user`, `password`) ou usa um bean da aplicação, que vale só quando o provider não
define esses campos. Com mais de um provider do mesmo tipo, o bean precisa se chamar `<provider>` ou
`<provider><Tipo>`. O resultado é um comportamento por provider: aviso no AWS, falha no Azure, JMS e RabbitMQ,
nenhuma saída no GCP, e falha no AWS só com os dois beans.

Três fatos mudam a conta:

1. **Ninguém usa dois providers do mesmo tipo.** A regra de nomes existe só para esse caso.
2. **Os serviços querem a conexão na aplicação.** O enginetax ainda não declara `SqsClient` nem credenciais, mas a
   preferência é que região, credencial, timeout e pool fiquem no código dele, junto do resto da sua infraestrutura
   de runtime. É a mesma linha do ADR-0001 para filas e tópicos, que são do Terraform.
3. **`access-key`/`secret-key` no YAML é anti-padrão.** Em produção a credencial vem do ambiente (IAM role, IRSA,
   Workload Identity, Managed Identity). O caminho por YAML serve, na prática, a LocalStack e emuladores.

A granularidade dos adapters limita a mudança. Só o AWS consome um cliente por provider (`SqsClient`, `SnsClient`).
Os outros consomem objetos por destino, que o starter cria a partir de um objeto raiz:

| Provider | O adapter recebe | Criado pelo starter a partir de |
|---|---|---|
| AWS | `SqsClient`, `SnsClient` | (o próprio cliente) |
| Azure | `ServiceBusSenderClient`/`ReceiverClient` por fila ou subscription | `ServiceBusClientBuilder` |
| GCP | `Publisher` por tópico, `SubscriberStub` por subscription, `TopicAdminClient` | credenciais e canal gRPC |
| ActiveMQ Classic, Artemis | `jakarta.jms.Connection` | `jakarta.jms.ConnectionFactory` |
| RabbitMQ | `com.rabbitmq.client.Connection` | `com.rabbitmq.client.ConnectionFactory` |

"A aplicação declara os clientes" não pode significar um bean por destino, porque isso traria a topologia para o
código Java. Significa que a aplicação declara o objeto raiz.

## Decisão

### 1. A aplicação declara o objeto raiz; o starter nunca o cria

| `type` | Bean da aplicação | Obrigatório |
|---|---|---|
| `aws` | `SqsClient` | Quando algum sender ou receiver usa SQS |
| `aws` | `SnsClient` | Quando algum sender usa um ARN do SNS |
| `azure` | `ServiceBusClientBuilder` | Sim |
| `gcp` | `com.google.api.gax.core.CredentialsProvider` | Não: sem ele, Application Default Credentials |
| `gcp` | `com.google.api.gax.rpc.TransportChannelProvider` | Não: sem ele, o canal padrão do SDK (no emulador, a aplicação declara um canal sem TLS e um `NoCredentialsProvider`) |
| `activemq-classic`, `artemis` | `jakarta.jms.ConnectionFactory` | Sim |
| `rabbitmq` | `com.rabbitmq.client.ConnectionFactory` | Sim |

- O bean é procurado pelo tipo. Sem bean obrigatório, o startup falha com a chave do provider e o bean a declarar
  (`messaging.providers.aws: declare um bean software.amazon.awssdk.services.sqs.SqsClient (ADR-0011)`). Com mais de
  um bean do tipo, vale o `@Primary`; sem ele, o startup falha com a mesma chave.
- Os beans do AWS são resolvidos no primeiro sender ou receiver que precisa deles, ainda no startup. Uma aplicação
  que só publica no SNS não precisa de `SqsClient`.
- O starter continua criando os objetos por destino (senders e receivers do Azure, `Publisher`, `SubscriberStub` e
  `TopicAdminClient` do GCP, a `Connection` de JMS e RabbitMQ) e fecha só o que criou. Um bean da aplicação nunca é
  fechado pelo starter.
- Os helpers dos adapters continuam disponíveis para a aplicação montar o bean: `JmsDialect.connectionFactory(url)`
  aplica `reconnectAttempts=-1` e, no Artemis, `consumerWindowSize=0`.

### 2. O YAML do provider fica só com o endereço

- `messaging.providers.<nome>` aceita `type` em todos os tipos e `project` no GCP, que completa nomes curtos de tópico
  e subscription. `project` é endereço, não conexão.
- `region`, `endpoint`, `access-key`, `secret-key`, `connection-string`, `emulator-host`, `broker-url`, `user` e
  `password` saem. Uma chave dessas derruba o startup com uma mensagem própria, e não com o "chave desconhecida"
  genérico: `messaging.providers.aws.region foi removida (ADR-0011): configure o bean SqsClient na aplicação`.
  Todas as chaves removidas aparecem na mesma mensagem.

### 3. Um provider por tipo de cliente

- Dois providers que usam o mesmo cliente nativo derrubam o startup. `activemq-classic` e `artemis` contam como um
  tipo (`jakarta.jms.ConnectionFactory`). Providers de tipos diferentes continuam juntos, que é o caso de migração do
  ADR-0008 (AWS e Azure ao mesmo tempo).
- A convenção de nomes `<provider>`/`<provider><Tipo>`, o aviso de bean ignorado, a falha de "properties e beans
  juntos" e o `onlyOfItsType` saem.
- Quando aparecer um caso real com dois providers do mesmo tipo, o caminho é uma referência explícita no YAML
  (`messaging.providers.conta-b.client: sqsContaB`), não a volta da convenção de nomes.

### 4. Invariantes que passam a ser da aplicação

- **Read timeout do AWS acima do long poll.** O receiver SQS espera até 20 s (`MAX_WAIT_SECONDS`). O starter
  configurava um Apache HTTP client com socket timeout de 30 s. Agora o `SqsClient` é da aplicação, e um timeout
  abaixo de 20 s corta o long poll e aparece como falha intermitente de receive. O SDK não expõe o timeout de um
  cliente já construído, então o starter não tem como validar isso. O README documenta o requisito e dá um exemplo de
  `@Configuration`.
- **`ConnectionFactory` JMS sem pool nem cache.** O `checkAccess` precisa da conexão do cliente do broker. Um
  `CachingConnectionFactory` ou `pooled-jms` por cima derruba o health check. O autoconfigure de JMS do Boot só cria
  a factory dele (com cache por padrão) quando a aplicação não declara nenhum `ConnectionFactory`: declarar o da lib o
  desliga (conferido nas fontes do Boot 4.1.1). O README diz isso.
- **Janela do Artemis.** A `ConnectionFactory` precisa de `consumerWindowSize=0`; o receiver continua conferindo e
  lança `IllegalArgumentException` sem ela, salvo quando a conexão não é a do cliente do broker (cache ou pool por cima:
  a checagem é pulada). `JmsDialect.ARTEMIS.connectionFactory(url)` já aplica o valor.
- **Reconexão e credenciais** de JMS e RabbitMQ são da factory da aplicação.

## Alternativas consideradas

- **Manter o ADR-0010.** O caminho por YAML continua útil para emuladores, mas mantém dois donos para a conexão e
  regras que variam por provider, só para servir a um caso (dois providers do mesmo tipo) que ninguém tem.
- **Um bean por destino** (`Publisher`, `ServiceBusSenderClient`). Traz a topologia para o Java e obriga a aplicação a
  saber como cada adapter usa o SDK.
- **Delegar ao Spring Cloud AWS/Azure/GCP e ao autoconfigure do Boot.** Eles não entregam os tipos que os adapters
  consomem: o Spring Cloud GCP expõe `PubSubTemplate`, o RabbitMQ do Boot é Spring AMQP e não expõe a
  `ConnectionFactory` nativa, e o Spring Cloud Azure não cria um sender por fila da lib. O ADR-0001 também deixa a lib
  sem Spring Cloud. Quem usar Spring Cloud AWS ganha um `SqsClient` que serve como bean deste ADR, sem trabalho extra.
- **Customizers (`ProviderCustomizer<SqsClientBuilder>`).** Mantêm o starter como dono do cliente, que é o oposto do
  que se quer aqui.
- **Um factory da lib (`MessagingClients.sqs(region)`)** que já aplica o timeout. Fica para quando um segundo serviço
  repetir o mesmo `@Configuration`. Hoje um exemplo compilado em `src/test` (`exemplos/`) basta.

## Consequências

- Trocar de nuvem passa a exigir trocar a dependência, o YAML e o `@Configuration` que declara o cliente. A promessa
  de "trocar de provedor sem mudar código" (ADR-0001) vale para o código de envio e consumo, não para a montagem da
  conexão.
- Quebra de configuração: as chaves removidas derrubam o startup com a mensagem de migração. Quebra de compilação
  para quem usa os records de `MessagingProperties` e os construtores dos `*ProviderConnection` na API Java. A lib
  ainda não tem versão estável publicada.
- Os testes do starter com LocalStack, emuladores e brokers declaram os beans no contexto de teste, como a aplicação
  vai fazer.
- Adicionar um provider passa a ser: o record, as linhas em `Endpoints.ACCEPTED`, o `ProviderConnection` e a linha da
  tabela da seção 1.
- Uma pendência do ADR-0010 vira erro explícito: `aws-prod` (YAML) e `awsprod` (variável de ambiente) eram dois
  providers em silêncio. Com o mesmo `type`, agora falham pela seção 3.

## Implementação (2026-11-15)

Entregue em `feat/clientes-da-aplicacao`, com testes escritos antes do código e uma revisão final independente (dois
achados Important e cinco Minor, todos corrigidos). O starter tem hoje 173 testes, com Docker
(LocalStack, Artemis e RabbitMQ). Diferenças em relação ao texto acima, decididas na implementação:

- `AwsProviderConnection`, `AzureProviderConnection` e `RabbitProviderConnection` não recebem mais o record do
  provider, que ficou sem campos. `JmsProviderConnection` o mantém por causa de `type()` (escolhe o dialeto) e
  `GcpProviderConnection` por causa de `project`.
- No AWS, cada cliente é resolvido no primeiro sender ou receiver que o usa, sem lock: duas threads podem resolver ao
  mesmo tempo e recebem o mesmo singleton.
- A mensagem de chave removida diz o que declarar por tipo: AWS, "o bean SqsClient (e o SnsClient, se enviar ao
  SNS)"; Azure, `ServiceBusClientBuilder`; GCP, `TransportChannelProvider` e `CredentialsProvider`; JMS,
  `jakarta.jms.ConnectionFactory`; RabbitMQ, `com.rabbitmq.client.ConnectionFactory`.
- Os problemas de `messaging.providers` (chaves removidas, chave desconhecida, campo de outro tipo) saem juntos em
  `Configuração inválida de messaging.providers:`, um por linha. `type` ausente continua abortando no primeiro erro.
- A `Connection` JMS é fechada se `start()` lança. Antes ela vazava.
- Os exemplos de cada provedor (`AwsClientsExample`, `AzureClientsExample`, `GcpClientsExample`, `JmsClientsExample` e
  `RabbitClientsExample`) ficam compilados em `messaging-spring-boot-starter/src/test/java/.../exemplos/`, com a forma de
  produção e, onde existe, a de emulador, e os testes de auto-configuração os usam (2026-11-16). Substituem o
  `@Configuration` solto que estava no README, como na storage-library.
- O teste de "GCP sem beans usa o padrão do SDK" ficou de fora: depende de Application Default Credentials da
  máquina e não é determinístico.

### Verificado

- **Read timeout do AWS.** O padrão do AWS SDK 2.55.6 é 30 s (`SdkHttpConfigurationOption.READ_TIMEOUT`), acima dos
  20 s do long poll. Só um `SqsClient` configurado abaixo de 20 s causa o problema.
- **Autoconfigure de JMS do Spring Boot 4.1.1** (fontes de `spring-boot-activemq` e `spring-boot-artemis`): a
  `jmsConnectionFactory` do Boot só existe quando a aplicação não declara nenhum `ConnectionFactory`
  (`@ConditionalOnMissingBean`). Por padrão é um `CachingConnectionFactory` (`spring.jms.cache.enabled`, padrão
  `true`); com `spring.activemq.pool.enabled` ou `spring.artemis.pool.enabled`, um `JmsPoolConnectionFactory`. Declarar
  a factory da lib desliga a do Boot; `@Primary` não é necessário.
- **ActiveMQ Classic atrás de um `CachingConnectionFactory`** (Spring 7.0.9, com as propriedades padrão do Boot,
  rodado fora do repositório): envio e consumo funcionam, e o `checkAccess` falha com `UnsupportedOperationException`
  nos dois lados.
- **Artemis, janela do consumer.** O padrão do cliente Artemis 2.40.0 é 1 MiB
  (`ActiveMQClient.DEFAULT_CONSUMER_WINDOW_SIZE`) e o Boot não define a janela.

### Não verificado

- **Artemis atrás de cache ou pool.** Pelo código, `ArtemisSupport.requireNoWindow` só confere a janela quando a
  conexão é um `ActiveMQConnection`, então um cache ou pool pula a checagem e o receiver pré-busca mensagens sem
  lease. Não foi rodado contra um broker.
- **`pooled-jms` (`JmsPoolConnectionFactory`)** não foi exercitado, só o `CachingConnectionFactory`. Espera-se o mesmo
  efeito no health check.
- **Spring Cloud AWS.** A afirmação de que o `SqsClient` dele serve como bean deste ADR vem da API, não de um teste.
- **O resto da aplicação.** O bean da lib vira o `ConnectionFactory` único, então `JmsTemplate` e `@JmsListener`
  também o usam. Não se mediu o efeito de uma factory sem cache nesse código.

### Pendências conhecidas

- O `TopicAdminClient` do GCP é criado no construtor: sem credenciais padrão nem bean de credenciais, o startup falha
  mesmo sem destinos.
- Um `SqsClient` com read timeout abaixo de 20 s não é detectado; o SDK não expõe o timeout de um cliente pronto.
- Falta um teste que prove que o starter nunca fecha o canal gRPC da aplicação: com um provider de canal fixo, o
  único caminho para fechá-lo é o próprio SDK.
- `type` ausente em um provider aborta o bind no primeiro erro, e a API Java ainda aceita um record com `type`
  inconsistente (`new AwsProvider(Type.GCP)`), pendências herdadas do ADR-0010.
- Quebra de compilação para quem usa os records de `MessagingProperties` e os construtores dos `*ProviderConnection`.
  Quebra de configuração para quem tinha `region`, `broker-url` e as outras chaves no YAML: ver "Migrando do ADR-0010"
  no README.
