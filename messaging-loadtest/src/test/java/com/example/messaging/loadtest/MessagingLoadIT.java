package com.example.messaging.loadtest;

import org.junit.jupiter.api.Test;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Instant;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

/**
 * Teste de carga do listener contra brokers reais. Só roda com {@code -Ploadtest}.
 *
 * <p>É um relatório, não um gate: falha só se um profile não drenar dentro do próprio {@code timeoutSeconds}.
 *
 * <pre>
 * mvn -pl messaging-loadtest -am verify -Ploadtest                                  # todos os profiles
 * mvn -pl messaging-loadtest -am verify -Ploadtest -Dloadtest.profiles=inmemory-perfeito,rabbit-real
 * mvn ... -Dloadtest.profilesDir=/caminho/meus-profiles
 * </pre>
 */
class MessagingLoadIT {

    private static final System.Logger LOG = System.getLogger(MessagingLoadIT.class.getName());

    @Test
    void executaProfilesEGeraRelatorio() throws Exception {
        List<LoadProfile> profiles = carregarProfiles();
        assertTrue(!profiles.isEmpty(), "nenhum profile encontrado");
        profiles.forEach(LoadProfile::validate);

        List<Map<String, Object>> results = new ArrayList<>();
        Path dir = Path.of(System.getProperty("loadtest.reportDir", "target/loadtest"),
                Instant.now().toString().replace(':', '-'));
        Map<String, List<LoadProfile>> porBroker = profiles.stream()
                .collect(Collectors.groupingBy(LoadProfile::broker, LinkedHashMap::new, Collectors.toList()));
        Throwable falhaDeBroker = null;
        LoadRunner runner = new LoadRunner();
        for (var entry : porBroker.entrySet()) {
            // um container por tipo de broker, reaproveitado entre os profiles dele
            try (BrokerFixture fixture = BrokerFixture.create(entry.getKey())) {
                for (LoadProfile p : entry.getValue()) {
                    LOG.log(System.Logger.Level.INFO, "profile {0} ({1})", p.name(), p.broker());
                    Map<String, Object> r = runner.run(p, fixture);
                    Map<String, Object> row = new LinkedHashMap<>();
                    row.put("profile", p.name());
                    row.put("config", p.props());
                    row.put("result", r);
                    results.add(row);
                    LOG.log(System.Logger.Level.INFO,
                            "profile {0}: drained={1} produce={2} msg/s consume={3} msg/s e2eMs={4} handlerMs={5} "
                                    + "util={6} dup={7} outOfOrder={8} effects={9}",
                            p.name(), r.get("drained"), r.get("producedPerSec"), r.get("throughputMsgPerSec"),
                            r.get("e2eMs"), r.get("handlerMs"), r.get("workerUtilization"), r.get("duplicates"),
                            r.get("outOfOrder"), r.get("terminalByEffect"));
                }
            } catch (Exception e) {
                // sem Docker, por exemplo: registra e segue com os demais brokers
                LOG.log(System.Logger.Level.ERROR, "broker " + entry.getKey() + " falhou", e);
                if (falhaDeBroker == null) {
                    falhaDeBroker = new IllegalStateException("broker '" + entry.getKey() + "' falhou: " + e, e);
                }
            }
        }
        Report.write(dir, results);
        LOG.log(System.Logger.Level.INFO, "relatório em {0}", dir.toAbsolutePath());
        if (falhaDeBroker != null) {
            throw new AssertionError(falhaDeBroker);
        }
        for (Map<String, Object> row : results) {
            Object drained = ((Map<?, ?>) row.get("result")).get("drained");
            if (!Boolean.TRUE.equals(drained)) {
                fail("profile " + row.get("profile") + " não drenou no prazo");
            }
        }
    }

    private static List<LoadProfile> carregarProfiles() throws Exception {
        Path dir = Path.of(System.getProperty("loadtest.profilesDir", "src/test/resources/profiles"));
        String wanted = System.getProperty("loadtest.profiles", "");
        List<String> names = wanted.isBlank() || wanted.startsWith("${") ? List.of() : List.of(wanted.split(","));
        try (Stream<Path> files = Files.list(dir)) {
            List<Path> sorted = files.filter(f -> f.toString().endsWith(".properties")).sorted().toList();
            List<LoadProfile> out = new ArrayList<>();
            for (Path f : sorted) {
                LoadProfile p = LoadProfile.load(f);
                if (names.isEmpty() || names.contains(p.name())) {
                    out.add(p);
                }
            }
            return out;
        }
    }
}
