# ADR-0013: Stale check por versão do aggregate

- Status: aceito (2026-10-08)
- Data: 2026-10-07
- Revisão: 2026-10-07 (ordem do pipeline com expiração e circuit breaker dentro do nível 1)

## Contexto

A idempotência (ADR-0004) evita processar a **mesma** mensagem duas vezes. Ela
não evita processar uma mensagem **velha** depois de uma mais nova do mesmo
aggregate. Isso acontece com:

- o nível 3 e o redrive, que devolvem mensagens horas depois (ADR-0002,
  ADR-0012);
- `on-exhausted: dead-letter` em `strict`, que aceita um buraco na sequência
  (ADR-0005);
- `relaxed`, em que a ordem nunca foi garantida.

A outbox-library publica `aggregateType`, `aggregateId` e `aggregateVersion`,
que o envelope expõe (ADR-0003).

## Decisão

### Regra

- Middleware opcional, depois da idempotência e antes do retry (a expiração
  por idade, ADR-0015, vem antes da idempotência):

  ```
  observation -> envelope -> expiração -> idempotência -> stale check -> retry nível 1 [ circuit breaker -> handler -> classificação ]
  ```

- Mensagem com `aggregateVersion <= última versão processada` do
  `(escopo, aggregateType, aggregateId)`: `Ack` sem chamar o handler,
  `outcome=stale`. A chave de idempotência é concluída (`complete`).
- Sucesso do handler: a última versão passa a ser a da mensagem, só se for
  maior (atualização condicional).
- Mensagem sem `aggregateId` ou sem `aggregateVersion`: o stale check não se
  aplica, e a mensagem segue.
- Versão maior que `última + 1` (buraco) é processada normalmente. Detectar
  buraco e esperar fica fora.
- Escopo = nome do consumer, o mesmo da idempotência.

### SPI

```java
public interface AggregateVersionStore {
    OptionalLong lastVersion(String scope, String aggregateType, String aggregateId);
    /** Grava {@code version} só se for maior que a atual. Devolve {@code false} se não gravou. */
    boolean advance(String scope, String aggregateType, String aggregateId, long version);
}
```

| Store | Módulo | Detalhe |
|---|---|---|
| Memória | `inbox-core` | Testes e instância única |
| JDBC | starter | Tabela `inbox_aggregate_version(scope, aggregate_type, aggregate_id, version, updated_at)`, PK nas três primeiras. `advance` com upsert `WHERE version < ?`. DDL no README; a lib não cria tabela |
| Redis | starter | Script Lua com `GET` e `SET` condicionais; chave `inbox:version:<scope>:<type>:<id>` |

### Concorrência

- Com JDBC no mesmo `DataSource` do negócio e transação em `join`: o
  `lastVersion` usa `SELECT ... FOR UPDATE` e o `advance` entra na transação
  do handler. Duas mensagens do mesmo aggregate em paralelo ficam serializadas
  pelo lock da linha, e a verificação é exata.
- Redis ou JDBC sem `join`: a verificação é otimista. Duas mensagens do mesmo
  aggregate processadas ao mesmo tempo podem passar as duas, e a mais velha
  pode ser aplicada depois da mais nova. Em `strict` isso não acontece, porque
  a mesma chave não é processada em paralelo (ADR-0005).
- O `advance` que devolve `false` (outra mensagem gravou versão maior durante o
  handler) só gera log de aviso e `inbox_stale_race_total`: o handler já rodou.

### TTL

O registro de versão não expira por padrão: uma versão velha pode chegar a
qualquer momento por redrive. `stale-check.ttl` permite expirar, para
aggregates de vida curta.

### Configuração

```yaml
messaging:
  receivers:
    billing-orders:
      stale-check:
        enabled: true
        store: jdbc          # memory | jdbc | redis
        ttl: null            # sem expiração
```

## Alternativas descartadas

- Reaproveitar a store de idempotência: a chave é diferente (aggregate e não
  mensagem), o valor é um número com atualização condicional e o registro não
  pode expirar junto com a idempotência.
- Esperar a versão que falta antes de processar (resequencer): exige guardar
  mensagens fora do broker e um timeout para buracos que nunca vão chegar.

## Consequências

- Nível 3 e redrive ficam seguros para aggregates versionados.
- O handler continua responsável por eventos sem versão.
- A tabela JDBC cresce com o número de aggregates. Sem TTL, ela precisa de
  limpeza pela aplicação quando o aggregate deixa de existir.
