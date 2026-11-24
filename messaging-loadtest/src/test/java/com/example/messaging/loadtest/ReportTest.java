package com.example.messaging.loadtest;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class ReportTest {

    @TempDir
    Path dir;

    @Test
    void jsonEscapaEAninha() {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("a", "x\"y\\z\nw");
        m.put("b", List.of(1, 2.5, true));
        m.put("c", Map.of("d", "e"));
        m.put("n", null);
        assertEquals("{\"a\":\"x\\\"y\\\\z\\nw\",\"b\":[1,2.5,true],\"c\":{\"d\":\"e\"},\"n\":null}", Report.json(m));
    }

    @Test
    void csvTemCabecalhoEUmaLinhaPorProfileComCampoAusenteVazio() throws Exception {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put("drained", true);
        result.put("producedPerSec", 100.0);
        Map<String, Object> row = new LinkedHashMap<>();
        row.put("profile", "p1");
        row.put("config", Map.of("broker", "inmemory", "messages", "10", "handlerLatency", "lognormal:p50=5ms,p99=50ms"));
        row.put("result", result); // sem e2eMs: consume=false
        Report.write(dir, List.of(row));
        List<String> lines = Files.readAllLines(dir.resolve("summary.csv"));
        assertEquals(2, lines.size());
        assertTrue(lines.get(0).startsWith("profile,broker,messages,"));
        assertTrue(lines.get(1).contains("\"lognormal:p50=5ms,p99=50ms\""));
        String[] cols = lines.get(1).replace("\"lognormal:p50=5ms,p99=50ms\"", "L").split(",", -1);
        assertEquals(lines.get(0).split(",").length, cols.length);
        assertEquals("p1", cols[0]);
        assertEquals("inmemory", cols[1]);
        assertTrue(Files.readString(dir.resolve("report.json")).contains("\"profile\":\"p1\""));
    }
}
