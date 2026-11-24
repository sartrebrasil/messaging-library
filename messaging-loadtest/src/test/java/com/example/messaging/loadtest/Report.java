package com.example.messaging.loadtest;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;

/** Grava {@code report.json} e {@code summary.csv}; o JSON é escrito à mão para não somar dependência. */
final class Report {

    private static final List<String> COLUMNS = List.of("profile", "broker", "messages", "producers", "orderingKeys",
            "concurrency", "maxMessagesPerPoll", "keyLanes", "handlerLatency", "transientFailureRate", "poisonRate",
            "producedPerSec", "throughputMsgPerSec", "e2eP50", "e2eP95", "e2eP99", "handlerP50", "handlerP99",
            "workerUtilization", "duplicates", "outOfOrder", "receiveFailed", "maxBacklog", "drained");

    private Report() {
    }

    @SuppressWarnings("unchecked")
    static void write(Path dir, List<Map<String, Object>> rows) throws IOException {
        Files.createDirectories(dir);
        Files.writeString(dir.resolve("report.json"), json(rows));
        StringBuilder sb = new StringBuilder(String.join(",", COLUMNS)).append('\n');
        for (Map<String, Object> row : rows) {
            Map<String, String> c = (Map<String, String>) row.get("config");
            Map<String, Object> r = (Map<String, Object>) row.get("result");
            Map<String, Object> e2e = map(r.get("e2eMs"));
            Map<String, Object> handler = map(r.get("handlerMs"));
            List<String> cells = List.of(
                    String.valueOf(row.get("profile")), c.getOrDefault("broker", "inmemory"),
                    c.getOrDefault("messages", ""), c.getOrDefault("producers", ""), c.getOrDefault("orderingKeys", ""),
                    c.getOrDefault("consumer.concurrency", ""), c.getOrDefault("consumer.maxMessagesPerPoll", ""),
                    c.getOrDefault("consumer.keyLanes", ""), c.getOrDefault("handlerLatency", ""),
                    c.getOrDefault("transientFailureRate", ""), c.getOrDefault("poisonRate", ""),
                    cell(r.get("producedPerSec")), cell(r.get("throughputMsgPerSec")), cell(e2e.get("p50")),
                    cell(e2e.get("p95")), cell(e2e.get("p99")), cell(handler.get("p50")), cell(handler.get("p99")),
                    cell(r.get("workerUtilization")), cell(r.get("duplicates")), cell(r.get("outOfOrder")),
                    cell(r.get("receiveFailed")), cell(r.get("maxBacklog")), cell(r.get("drained")));
            sb.append(cells.stream().map(Report::csv).collect(java.util.stream.Collectors.joining(","))).append('\n');
        }
        Files.writeString(dir.resolve("summary.csv"), sb.toString());
    }

    @SuppressWarnings("unchecked")
    private static Map<String, Object> map(Object o) {
        return o instanceof Map<?, ?> m ? (Map<String, Object>) m : Map.of();
    }

    /** Aspas quando o valor tem vírgula ou aspas (ex.: handlerLatency lognormal:p50=5ms,p99=50ms). */
    private static String csv(String v) {
        return v.contains(",") || v.contains("\"") ? "\"" + v.replace("\"", "\"\"") + "\"" : v;
    }

    private static String cell(Object o) {
        return o == null ? "" : String.valueOf(o);
    }

    static String json(Object v) {
        StringBuilder sb = new StringBuilder();
        append(sb, v);
        return sb.toString();
    }

    private static void append(StringBuilder sb, Object v) {
        switch (v) {
            case null -> sb.append("null");
            case Map<?, ?> m -> {
                sb.append('{');
                boolean first = true;
                for (Map.Entry<?, ?> e : m.entrySet()) {
                    if (!first) {
                        sb.append(',');
                    }
                    first = false;
                    string(sb, String.valueOf(e.getKey()));
                    sb.append(':');
                    append(sb, e.getValue());
                }
                sb.append('}');
            }
            case List<?> l -> {
                sb.append('[');
                for (int i = 0; i < l.size(); i++) {
                    if (i > 0) {
                        sb.append(',');
                    }
                    append(sb, l.get(i));
                }
                sb.append(']');
            }
            case Number n -> sb.append(n);
            case Boolean b -> sb.append(b);
            default -> string(sb, v.toString());
        }
    }

    private static void string(StringBuilder sb, String s) {
        sb.append('"');
        for (char ch : s.toCharArray()) {
            switch (ch) {
                case '"' -> sb.append("\\\"");
                case '\\' -> sb.append("\\\\");
                case '\n' -> sb.append("\\n");
                case '\r' -> sb.append("\\r");
                case '\t' -> sb.append("\\t");
                default -> {
                    if (ch < 0x20) {
                        sb.append(String.format("\\u%04x", (int) ch));
                    } else {
                        sb.append(ch);
                    }
                }
            }
        }
        sb.append('"');
    }
}
