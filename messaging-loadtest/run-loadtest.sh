#!/bin/sh
# Roda o teste de carga do InboxConsumer e resume o resultado broker a broker, profile a profile.
#
#   ./run-loadtest.sh                                  # todos os profiles (precisa de Docker para rabbit, sqs, jms)
#   ./run-loadtest.sh inmemory-perfeito,rabbit-real    # só alguns profiles (nomes dos arquivos, sem .properties)
#   PROFILES_DIR=/caminho/meus-profiles ./run-loadtest.sh
#   MVN_FLAGS=-o ./run-loadtest.sh                     # flags extras do maven (ex.: offline)
#   ./run-loadtest.sh sweep [rabbit,sqs,jms-classic]   # varredura de concurrency x maxMessagesPerPoll x keyLanes
#
# sweep: gera profiles temporários (handler lognormal p50=5ms/p99=50ms, sem falhas, preload=true para medir só o
# consumo) e roda cada combinação por broker; o fim do resumo aponta a melhor combinação de cada um. O JMS Classic
# não roda maxMessagesPerPoll=10 (achado conhecido, ~13 msg/s: veja o profile jms-classic-multipoll). A latência
# ponta a ponta do sweep inclui a espera do preload: compare só o consume.
#
# O log completo fica em messaging-loadtest/target/loadtest-last.log e o relatório (report.json, summary.csv)
# em messaging-loadtest/target/loadtest/<timestamp>/.
set -u

ROOT=$(cd "$(dirname "$0")/.." && pwd)
LOG="$ROOT/messaging-loadtest/target/loadtest-last.log"
mkdir -p "$ROOT/messaging-loadtest/target"

MODE=${1:-}
BROKERS=${2:-rabbit,sqs,jms-classic}
PROFILES=${1:-}
if [ "$MODE" = sweep ]; then
    PROFILES=
    PROFILES_DIR=$(mktemp -d)
    trap 'rm -rf "$PROFILES_DIR"' EXIT
    for b in $(echo "$BROKERS" | tr ',' ' '); do
        msgs=3000
        [ "$b" = jms-classic ] && msgs=1000
        for cfg in "1 1 1" "4 1 1" "16 1 1" "1 10 1" "4 10 1" "16 10 1" "4 10 4"; do
            c=${cfg%% *}; rest=${cfg#* }; m=${rest%% *}; l=${rest#* }
            [ "$b" = jms-classic ] && [ "$m" -gt 1 ] && continue
            cat > "$PROFILES_DIR/sweep-$b-c$c-m$m-l$l.properties" <<EOF
broker=$b
messages=$msgs
producers=4
orderingKeys=100
preload=true
consumer.concurrency=$c
consumer.maxMessagesPerPoll=$m
consumer.keyLanes=$l
handlerLatency=lognormal:p50=5ms,p99=50ms
timeoutSeconds=120
EOF
        done
    done
fi

set -- -pl messaging-loadtest -am verify -Ploadtest -Dtest=none -Dsurefire.failIfNoSpecifiedTests=false \
    ${MVN_FLAGS:-} ${PROFILES:+-Dloadtest.profiles=$PROFILES} ${PROFILES_DIR:+-Dloadtest.profilesDir=$PROFILES_DIR}

echo "mvn $*"
echo "(rodando; log em $LOG)"
(cd "$ROOT" && mvn "$@") > "$LOG" 2>&1
STATUS=$?

# Linhas do log: "INFO: profile NOME (broker)" no início e "INFO: profile NOME: drained=... " no fim de cada profile.
awk '
function field(line, key,    i, rest, e) {
    i = index(line, key "=")
    if (i == 0) return "-"
    rest = substr(line, i + length(key) + 1)
    if (substr(rest, 1, 1) == "{") { e = index(rest, "}"); return substr(rest, 1, e) }
    e = index(rest, " ")
    return e ? substr(rest, 1, e - 1) : rest
}
function p(map, key,    i, rest, e) {
    i = index(map, key "=")
    if (i == 0) return "-"
    rest = substr(map, i + length(key) + 1)
    e = match(rest, /[,}]/)
    return e ? substr(rest, 1, e - 1) : rest
}
/^INFO: profile [^ :]+ \([a-z-]+\)$/ {
    name = $3; b = $4; gsub(/[()]/, "", b); broker[name] = b
    if (!(b in seen)) { seen[b] = 1; order[++nb] = b }
    next
}
/^INFO: profile [^ ]+: drained=/ {
    name = $3; sub(/:$/, "", name)
    b = broker[name]
    line = $0
    n[b]++
    c = field(line, "consume"); gsub(/,/, "", c); c += 0
    if (field(line, "drained") == "true" && c > best[b]) { best[b] = c; bestName[b] = name }
    row[b, n[b]] = sprintf("  %-24s drained=%-5s produce=%s msg/s  consume=%s msg/s\n" \
        "  %-24s e2e p50=%s p99=%s ms | handler p50=%s p99=%s ms | util=%s | dup=%s outOfOrder=%s\n" \
        "  %-24s effects=%s\n", name, field(line, "drained"), field(line, "produce"), field(line, "consume"),
        "", p(field(line, "e2eMs"), "p50"), p(field(line, "e2eMs"), "p99"),
        p(field(line, "handlerMs"), "p50"), p(field(line, "handlerMs"), "p99"),
        field(line, "util"), field(line, "dup"), field(line, "outOfOrder"), "", field(line, "effects"))
}
END {
    if (nb == 0) { print "\nnenhum profile concluído; veja o log"; exit }
    print ""
    for (i = 1; i <= nb; i++) {
        b = order[i]
        printf "[%s]\n", b
        for (j = 1; j <= n[b]; j++) printf "%s\n", row[b, j]
        if (n[b] > 1 && best[b] > 0) printf "  melhor consumo de %s: %s (%s msg/s)\n\n", b, bestName[b], best[b]
    }
}' "$LOG"

if [ "$STATUS" -ne 0 ]; then
    echo "FALHOU (exit $STATUS). Últimas linhas de erro:"
    grep -E "ERROR|falhou|não drenou" "$LOG" | head -8
else
    echo "OK. Relatório: $(ls -dt "$ROOT"/messaging-loadtest/target/loadtest/*/ 2>/dev/null | head -1)"
fi
exit "$STATUS"
