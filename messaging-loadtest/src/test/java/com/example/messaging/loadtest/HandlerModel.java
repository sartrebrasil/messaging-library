package com.example.messaging.loadtest;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Handler simulado: latência configurável e falhas decididas por hash de ({@code seed}, id), então duas execuções
 * do mesmo profile são comparáveis. {@code poison} falha sempre; {@code transient} falha nas primeiras
 * {@code transientFailAttempts} tentativas do id e depois passa. Os dois conjuntos são disjuntos.
 */
final class HandlerModel {

    private static final Pattern FIXED = Pattern.compile("fixed:(\\d+(?:\\.\\d+)?)ms");
    private static final Pattern UNIFORM = Pattern.compile("uniform:(\\d+(?:\\.\\d+)?)-(\\d+(?:\\.\\d+)?)ms");
    private static final Pattern LOGNORMAL = Pattern.compile(
            "lognormal:p50=(\\d+(?:\\.\\d+)?)ms,p99=(\\d+(?:\\.\\d+)?)ms");
    /** z do percentil 99 da normal padrão. */
    private static final double Z99 = 2.3263478740408408;

    private final double fixedMs;
    private final double uniformFromMs;
    private final double uniformToMs;
    private final double mu;
    private final double sigma;
    private final boolean lognormal;
    private final double poisonRate;
    private final double transientRate;
    private final int transientAttempts;
    private final long seed;
    private final ConcurrentHashMap<String, AtomicInteger> attempts = new ConcurrentHashMap<>();

    private HandlerModel(String latency, double poisonRate, double transientRate, int transientAttempts, long seed) {
        this.poisonRate = poisonRate;
        this.transientRate = transientRate;
        this.transientAttempts = transientAttempts;
        this.seed = seed;
        double fixed = 0;
        double from = -1;
        double to = -1;
        double mu = 0;
        double sigma = 0;
        boolean logn = false;
        if (!"0".equals(latency)) {
            Matcher f = FIXED.matcher(latency);
            Matcher u = UNIFORM.matcher(latency);
            Matcher l = LOGNORMAL.matcher(latency);
            if (f.matches()) {
                fixed = Double.parseDouble(f.group(1));
            } else if (u.matches()) {
                from = Double.parseDouble(u.group(1));
                to = Double.parseDouble(u.group(2));
                if (to < from) {
                    throw new IllegalArgumentException("handlerLatency inválida (faixa invertida): " + latency);
                }
            } else if (l.matches()) {
                double p50 = Double.parseDouble(l.group(1));
                double p99 = Double.parseDouble(l.group(2));
                if (p50 <= 0 || p99 < p50) {
                    throw new IllegalArgumentException("handlerLatency inválida (p99 deve ser >= p50 > 0): " + latency);
                }
                logn = true;
                mu = Math.log(p50);
                sigma = Math.log(p99 / p50) / Z99;
            } else {
                throw new IllegalArgumentException("handlerLatency inválida: '" + latency
                        + "' (use 0, fixed:20ms, uniform:5-50ms ou lognormal:p50=20ms,p99=200ms)");
            }
        }
        this.fixedMs = fixed;
        this.uniformFromMs = from;
        this.uniformToMs = to;
        this.mu = mu;
        this.sigma = sigma;
        this.lognormal = logn;
    }

    static HandlerModel from(LoadProfile p) {
        return new HandlerModel(p.str("handlerLatency", "0"), p.rate("poisonRate"), p.rate("transientFailureRate"),
                p.num("transientFailAttempts", 1), Long.parseLong(p.str("seed", "1")));
    }

    boolean isPoison(String id) {
        return unit(id) < poisonRate;
    }

    boolean isTransient(String id) {
        double u = unit(id);
        return u >= poisonRate && u < poisonRate + transientRate;
    }

    /** Dorme a latência e falha conforme a decisão do id. */
    void handle(String id) throws Exception {
        long nanos = sampleNanos(ThreadLocalRandom.current());
        if (nanos > 0) {
            LockSupport.parkNanos(nanos);
        }
        if (isPoison(id)) {
            throw new IllegalStateException("poison");
        }
        if (isTransient(id) && attempts.computeIfAbsent(id, k -> new AtomicInteger()).incrementAndGet()
                <= transientAttempts) {
            throw new IllegalStateException("transient");
        }
    }

    long sampleNanos(ThreadLocalRandom r) {
        double ms;
        if (lognormal) {
            ms = Math.exp(mu + sigma * r.nextGaussian());
        } else if (uniformFromMs >= 0) {
            ms = uniformFromMs + r.nextDouble() * (uniformToMs - uniformFromMs);
        } else {
            ms = fixedMs;
        }
        return (long) (ms * TimeUnit.MILLISECONDS.toNanos(1));
    }

    /** splitmix64 de (seed, id), mapeado para [0,1). */
    private double unit(String id) {
        long z = seed + 0x9E3779B97F4A7C15L * (id.hashCode() + 1L);
        z = (z ^ (z >>> 30)) * 0xBF58476D1CE4E5B9L;
        z = (z ^ (z >>> 27)) * 0x94D049BB133111EBL;
        z ^= z >>> 31;
        return (z >>> 11) * 0x1.0p-53;
    }
}
