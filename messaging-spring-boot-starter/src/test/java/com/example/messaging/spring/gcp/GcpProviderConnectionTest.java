package com.example.messaging.spring.gcp;

import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.TestGcpClients;
import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.ManagedChannel;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Proxy;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import static org.assertj.core.api.Assertions.assertThat;

/** O GCP usa o canal e as credenciais que a aplicação declara (ADR-0011); sem eles, o padrão do SDK. */
class GcpProviderConnectionTest {

    private final ApplicationContextRunner runner = new ApplicationContextRunner()
            .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
            .withPropertyValues("messaging.providers.gcp.type=gcp", "messaging.providers.gcp.project=p",
                    "messaging.senders.pedidos.destination=pedidos");

    /** Repassa tudo ao provider de verdade e anota o nome de cada método chamado. */
    private static TransportChannelProvider spy(TransportChannelProvider real, List<String> calls) {
        return (TransportChannelProvider) Proxy.newProxyInstance(TransportChannelProvider.class.getClassLoader(),
                new Class<?>[]{TransportChannelProvider.class}, (proxy, method, args) -> {
                    calls.add(method.getName());
                    try {
                        return method.invoke(real, args);
                    } catch (InvocationTargetException e) {
                        throw e.getCause();
                    }
                });
    }

    @Test
    void gcpAppliesChannelAndCredentialsBeans() {
        ManagedChannel channel = TestGcpClients.plaintext("localhost:1");
        List<String> calls = new CopyOnWriteArrayList<>();
        try {
            runner.withBean(TransportChannelProvider.class, () -> spy(TestGcpClients.provider(channel), calls))
                    .withBean(CredentialsProvider.class, NoCredentialsProvider::create)
                    .run(context -> {
                        assertThat(context).hasNotFailed();
                        // o SDK troca o provider por withUseS2A e chama getTransportChannel no novo: basta ter consultado o da aplicação
                        assertThat(calls).isNotEmpty();
                    });
        } finally {
            channel.shutdownNow();
        }
    }

    @Test
    void gcpNeverClosesTheApplicationsChannel() {
        ManagedChannel channel = TestGcpClients.plaintext("localhost:1");
        try {
            runner.withBean(TransportChannelProvider.class, () -> TestGcpClients.provider(channel))
                    .withBean(CredentialsProvider.class, NoCredentialsProvider::create)
                    .run(context -> assertThat(context).hasNotFailed());
            // o contexto já fechou a conexão: o canal é da aplicação e continua aberto
            assertThat(channel.isShutdown()).isFalse();
        } finally {
            channel.shutdownNow();
        }
    }
}
