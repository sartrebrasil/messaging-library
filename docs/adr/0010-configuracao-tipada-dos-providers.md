# ADR-0010: Configuração tipada dos providers no starter

- Status: aceito
- Data: 2026-11-14
- Relacionada: [ADR-0008](0008-spring-boot-e-observabilidade.md), [ADR-0009](0009-activemq-e-rabbitmq.md)

## Contexto

O starter cria o cliente nativo de cada provider a partir de `messaging.providers.<nome>` e traduz o endereço de
cada sender e receiver (`destination`, `subscription`, `kind`, `options`) nos campos que o adapter usa. Hoje isso
passa por `MessagingProperties`, `Endpoints`, `MessagingDestinations` e um `ProviderConnection` por tipo. A revisão
de 2026-11-14 achou cinco problemas:

1. **Um record achatado serve a todos os tipos.** `Provider` tem 11 campos e `Options` 12, e cada tipo usa só
   alguns. O binding estrito só rejeita chave desconhecida: `region` num provider `rabbitmq` é conhecida e é
   ignorada em silêncio. O que cada tipo aceita é checado à mão em `Endpoints.validateOptions`, cerca de 10
   condições "só vale para o provider X".
2. **Três traduções em cadeia.** `Endpoint` (a string `destination` muda de sentido conforme o tipo, por heurística
   de `https://`, `arn:aws:sns:`, `kind` e `subscription`) vira `Destination` (17 campos, a maioria nula), e cada
   `ProviderConnection` reinterpreta esse `Destination`.
3. **A regra do bean da aplicação muda por tipo.**

   | Provider | Quando o bean da aplicação vale |
   |---|---|
   | AWS | Sempre; vale para todos os providers `aws` e ignora `region`, `endpoint` e credenciais configurados |
   | Azure | Só sem `connection-string` |
   | JMS e RabbitMQ | Só sem `broker-url` |
   | GCP | Não existe a opção |

   Dois providers `aws` com um `SqsClient` declarado compartilham o mesmo cliente, sem aviso. É um bug latente.
4. **`MessagingDestinations` concentra responsabilidades** (validação, conexões, cache de senders, health,
   decorator de receiver). Adicionar um provider obriga a editar `ADAPTERS`, `MODULES`, `BROKER_CLIENTS`, o `switch`
   de `connect` e `Endpoints`. Os `ProviderConnection` retornam `null` em ramos que a validação já tornou
   inalcançáveis.
5. **Duplicação.** `AwsProviderConnection` tem dois `configure(...)` idênticos, para SQS e SNS.

## Decisão

Duas mudanças, sem alterar o formato do YAML:

### 1. Uma regra única para o bean da aplicação

- Um bean de cliente nativo (`SqsClient`, `SnsClient`, `ServiceBusClientBuilder`, `ConnectionFactory`) só substitui
  o cliente criado pelo starter quando o provider **não** define os campos de conexão: `region`, `endpoint`,
  `access-key` e `secret-key` no AWS; `connection-string` no Azure; `broker-url` no JMS e no RabbitMQ.
- No **AWS**, provider com esses campos **e** os dois beans (`SqsClient` e `SnsClient`) derruba o startup: "defina as
  properties ou os beans, não os dois". Hoje o bean vence e as properties são ignoradas em silêncio. Com um bean só,
  as properties ainda configuram o outro cliente (o `SnsClient` precisa de região), então não há perda e o startup
  segue.
- No Azure, JMS e RabbitMQ, `connection-string` e `broker-url` continuam vencendo o bean, sem falhar. O Boot cria
  `ConnectionFactory`s que não são da lib; falhar ali derrubaria quem configura `broker-url` de propósito.
- Com mais de um provider do mesmo tipo, o bean só vale se a aplicação o declarar com o nome do provider (`aws`)
  ou `<provider><Tipo>` (`awsSnsClient`: um provider AWS tem dois clientes, e dois beans não têm o mesmo nome); um
  bean sem nome correspondente não é compartilhado. (O bean com o nome do provider é o mecanismo escolhido; a
  alternativa descartada abaixo é o customizer.)
- O GCP continua sem escape hatch: as credenciais já são a cadeia padrão do SDK.

### 2. Um tipo de properties por provider

- `type` escolhe o record: `AwsProvider(region, endpoint, accessKey, secretKey)`,
  `AzureProvider(connectionString)`, `GcpProvider(project, emulatorHost)`,
  `JmsProvider(brokerUrl, user, password)` e `RabbitProvider(brokerUrl, user, password)`.
- O YAML continua plano. O starter lê `type` primeiro e liga o mesmo nó ao record do tipo, com
  `NoUnboundElementsBindHandler`. Campo de outro tipo derruba o startup, com a chave na mensagem.
- `Options` **continua um record achatado**: a inbox liga `messaging.receivers.*.options` antes de conhecer o provider
  (ele pode vir de `receiver-defaults`), então um record por tipo exigiria ligar `options` em duas fases. No lugar,
  `Endpoints.validateOptions` usa uma tabela `opção → providers que a aceitam`, que substitui as checagens "só vale
  para o provider X". A tabela também rejeita opções hoje ignoradas em silêncio: `ack-deadline`, `ordered` e
  `exactly-once` fora do GCP, `redelivery` no AWS e no GCP, `max-message-bytes` no GCP. Opção com o valor padrão
  (`false`, conjunto vazio) não conta como definida.
- `Destination` continua existindo.
- Os `return null` inalcançáveis saem de `sender` e `receiver`.
- O `AwsProviderConnection` passa a ter um só `configure`.

### Ordem

1. Regra do bean e unificação do `configure` (corrige o bug do ponto 3; não muda a API).
2. Records por tipo e `Endpoint` tipado.

## Alternativas consideradas

- **Customizers (`ProviderCustomizer<SqsClientBuilder>` etc.)**: o starter sempre constrói o cliente e aplica os
  beans customizer, como o Boot faz. É a regra mais uniforme, mas o Azure com AAD hoje depende do bean
  `ServiceBusClientBuilder` e passaria a exigir `namespace` mais um customizer de credencial. Fica para quando
  alguém precisar mexer no builder sem trocar o cliente; a regra do bean acima não impede a migração.
- **Remover a criação de clientes e exigir só beans da aplicação**: simples, mas não atende vários providers do mesmo
  tipo (duas contas AWS, migração entre brokers), que é o caso do ADR-0008.
- **Records de options por tipo**: descartada porque exige ligar `options` em duas fases e mexer em `Endpoint`,
  `InboxProperties`, `InboxConfig` e nos testes que constroem `Options`. A tabela de campos aceitos dá o mesmo
  resultado para o usuário.
- **Quebrar `MessagingDestinations`** em registry de conexões, resolvedor e cache: melhora a leitura, mas não
  resolve os problemas acima. Fica depois de 1 e 2, e o 2 já tira dele a validação por tipo.
- **`ServiceLoader` por provider**: já tentado e revertido na revisão de 2026-11-06 (amarra os adapters ao starter).

## Consequências

- Erro de configuração que hoje passa em silêncio (campo de outro tipo, opção que o provider não aceita, properties
  junto com bean no AWS) passa a derrubar o startup. Aplicações que dependiam disso quebram no deploy; a mensagem diz a chave e o que fazer.
- Quem usa só YAML não muda nada, a não ser pelos erros novos acima.
- Quem usa a API Java (`MessagingProperties.Provider` e os construtores dos `*ProviderConnection`) tem uma quebra de
  compilação. Não verificamos se há consumidor externo; a biblioteca ainda não tem versão publicada estável.
- Adicionar um provider passa a ser: um record de provider, as linhas dele na tabela `Endpoints.ACCEPTED` e um
  `ProviderConnection`, mais o `case` no `switch` de `connect`.

## Implementação (2026-11-14)

Entregue em `feat/providers-tipados` (merge `51c2831`). Diferenças em relação ao texto acima, decididas na
implementação:

- O nome do bean pode ser `<provider>` ou `<provider><Tipo>` (`aws`, `awsSnsClient`), porque um provider AWS tem dois
  clientes e dois beans não têm o mesmo nome.
- O conflito no AWS só falha com os dois beans. Com um só, as properties configuram o outro cliente.
- Um bean que o starter não usa (AWS, mais de um provider) gera um aviso; Azure, JMS e RabbitMQ falham com uma
  mensagem que diz como nomear o bean.
- Na tabela de opções, `false`, conjunto vazio e `redelivery: abandon` contam como não definidos.

### Pendências conhecidas

Achados de revisão deixados de fora, nenhum é regressão em relação ao comportamento anterior, exceto onde indicado:

- Variável de ambiente de provider sem `type` aponta o nome errado na mensagem (`MESSAGING_PROVIDERS_AWS_PROD_TYPE`
  vira `aws.prod`). Regressão de mensagem; o comportamento é o mesmo.
- Provider sem `type` aborta o bind no primeiro erro, em vez de entrar na lista de todos os problemas.
- A tabela de opções é por provider, não por direção: `max-message-bytes` num sender SQS e `confirm-timeout` num
  receiver RabbitMQ continuam ignorados em silêncio.
- `routing-key` e `routing-by-ordering-key` fora do RabbitMQ geram dois erros (um da tabela, um de combinação).
- Records de provider criados na API Java aceitam `type` inconsistente (`new AwsProvider(Type.GCP, ...)`) e falham
  com `ClassCastException` ou NPE; falta validar `type` no construtor compacto.
- Providers `aws-prod` (YAML) e `awsprod` (variável de ambiente) viram dois providers. Já era assim antes.
