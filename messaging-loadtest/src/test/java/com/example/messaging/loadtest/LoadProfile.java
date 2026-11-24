package com.example.messaging.loadtest;

import com.example.messaging.inbox.DelayedStrategy;
import com.example.messaging.inbox.Ordering;

import java.io.IOException;
import java.io.Reader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.TreeMap;

/** Um profile = um arquivo .properties; o nome do arquivo (sem extensão) é o nome do profile. */
record LoadProfile(String name, Map<String, String> props) {

    static LoadProfile load(Path file) throws IOException {
        Properties p = new Properties();
        try (Reader r = Files.newBufferedReader(file)) {
            p.load(r);
        }
        Map<String, String> map = new TreeMap<>();
        p.forEach((k, v) -> map.put(k.toString(), v.toString().trim()));
        String fileName = file.getFileName().toString();
        return new LoadProfile(fileName.substring(0, fileName.lastIndexOf('.')), map);
    }

    static LoadProfile of(String name, Map<String, String> props) {
        return new LoadProfile(name, new TreeMap<>(props));
    }

    String broker() {
        return str("broker", "inmemory");
    }

    String str(String key, String def) {
        return props.getOrDefault(key, def);
    }

    int num(String key, int def) {
        return Integer.parseInt(props.getOrDefault(key, String.valueOf(def)));
    }

    /** Taxa ou número fracionário; 0 quando ausente. */
    double rate(String key) {
        return Double.parseDouble(props.getOrDefault(key, "0"));
    }

    boolean flag(String key, boolean def) {
        return Boolean.parseBoolean(props.getOrDefault(key, String.valueOf(def)));
    }

    /** Recusa combinações contraditórias antes de subir qualquer broker. */
    void validate() {
        if (flag("preload", false) && rate("ratePerSec") > 0) {
            throw new IllegalArgumentException(name + ": preload=true não combina com ratePerSec>0");
        }
        double poison = rate("poisonRate");
        double transientRate = rate("transientFailureRate");
        if (poison < 0 || transientRate < 0 || poison + transientRate > 1) {
            throw new IllegalArgumentException(name + ": poisonRate e transientFailureRate devem ser >= 0 e somar até 1 ("
                    + poison + " + " + transientRate + ")");
        }
        for (String key : List.of("orderingKeys", "producers", "messages", "consumer.concurrency",
                "consumer.maxMessagesPerPoll", "consumer.keyLanes")) {
            if (num(key, 1) < 1) {
                throw new IllegalArgumentException(name + ": " + key + " deve ser >= 1");
            }
        }
        try {
            Ordering.valueOf(str("consumer.ordering", "RELAXED").toUpperCase());
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(name + ": consumer.ordering inválido: " + str("consumer.ordering", ""), e);
        }
        try {
            DelayedStrategy.valueOf(str("consumer.strategy", "AUTO").toUpperCase().replace('-', '_'));
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException(name + ": consumer.strategy inválido: " + str("consumer.strategy", ""), e);
        }
        HandlerModel.from(this); // handlerLatency inválida falha aqui, antes de subir o broker
        if (num("orderingKeys", 100) > num("messages", 1000)) {
            throw new IllegalArgumentException(name + ": orderingKeys (" + num("orderingKeys", 100)
                    + ") maior que messages (" + num("messages", 1000) + ")");
        }
    }
}
