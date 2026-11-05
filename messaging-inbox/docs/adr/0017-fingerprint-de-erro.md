# ADR-0017: Fingerprint de erro

- Status: aceito (2026-10-08)
- Data: 2026-10-07

## Contexto

Depois de um incidente, a DLQ e a `failed` podem ter dezenas de milhares de
mensagens. Para decidir o que reprocessar (ADR-0012), o operador precisa saber
quantas falharam pelo mesmo motivo. A classe da exceção não basta: um
`IllegalStateException` vem de dezenas de lugares do código. A mensagem da
exceção também não: muda a cada mensagem (ids, valores) e pode conter dado do
payload.

A acme-messaging, incorporada a este projeto (ADR-0006), tinha
`ErrorFingerprint`, implementado e testado.

## Decisão

### Cálculo

- Causa raiz: último elo da cadeia de `getCause()` (mesma travessia do
  ADR-0014).
- Texto: nome da classe da causa raiz e os 5 primeiros frames do stack trace,
  cada um como `classe#método`, **sem** número de linha. Assim o fingerprint
  sobrevive a mudanças triviais no arquivo.
- Fingerprint: SHA-1 do texto em UTF-8, os 12 primeiros caracteres em hex.
  Não é uso de segurança; o hash só agrupa.
- Sem exceção (`Reject` sem `cause`, ADR-0014): sem fingerprint.

### Onde aparece

| Lugar | Detalhe |
|---|---|
| Atributo `inbox_failure` (ADR-0002) | Novo campo `fingerprint=<hex>`. Não gasta atributo do orçamento de 16 |
| Span | Atributo `inbox.error.fingerprint` |
| Log de falha | Campo `fingerprint` |
| Redrive (ADR-0012) | Filtro por fingerprint e contagem por fingerprint no relatório |

- Proibido como tag de métrica. A cardinalidade depende de quantos caminhos de
  código falham, sem limite conhecido. As métricas continuam com `exception`
  (ADR-0007).

## Alternativas descartadas

- Fingerprint com número de linha: qualquer mudança no arquivo separaria
  falhas iguais entre duas versões da aplicação.
- Fingerprint pela mensagem da exceção: muda a cada mensagem e expõe dado do
  payload.
- SHA-256, como o fingerprint de payload do ADR-0004: o tamanho do hash não
  importa para agrupar, e o código da acme-messaging já existe com SHA-1.

## Consequências

- O operador agrupa a DLQ e a `failed` por causa real e reprocessa só o grupo
  do bug corrigido.
- Uma refatoração que muda nomes de classe ou método nos 5 primeiros frames
  muda o fingerprint. Falhas antigas e novas aparecem como grupos diferentes.
