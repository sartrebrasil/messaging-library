package com.example.messaging.spring.exemplos;

import com.google.api.gax.core.CredentialsProvider;
import com.google.api.gax.core.NoCredentialsProvider;
import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Como a aplicação configura o Pub/Sub para o starter com {@code type: gcp} (ADR-0011). O {@link TransportChannelProvider}
 * e o {@link CredentialsProvider} são beans opcionais: o starter os aplica ao cliente de administração, aos publishers
 * e aos subscribers que cria. Sem eles, vale o padrão do SDK (Application Default Credentials).
 */
public final class GcpClientsExample {

    private GcpClientsExample() {
    }

    /** Um canal sem TLS para o emulator do Pub/Sub, em {@code host:porta}. O canal é da aplicação. */
    public static ManagedChannel emulatorChannel(String hostAndPort) {
        return ManagedChannelBuilder.forTarget(hostAndPort).usePlaintext().build();
    }

    /** Fixo: o SDK não fecha o canal, que é de quem o criou. */
    public static TransportChannelProvider channelProvider(ManagedChannel channel) {
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    }

    /**
     * Produção: nenhum bean. As credenciais vêm de Application Default Credentials ({@code GOOGLE_APPLICATION_CREDENTIALS},
     * Workload Identity) e o canal é o padrão do SDK. Só declare um {@code CredentialsProvider} para trocar isso.
     */
    @Configuration(proxyBeanMethods = false)
    public static class Producao {
    }

    /** Emulador: o Pub/Sub emulator em {@code localhost:8085}, sem TLS e sem credenciais; o canal fecha com o contexto. */
    @Configuration(proxyBeanMethods = false)
    public static class Emulador {

        @Bean(destroyMethod = "shutdownNow")
        ManagedChannel emulatorChannel() {
            return GcpClientsExample.emulatorChannel("localhost:8085");
        }

        @Bean
        TransportChannelProvider transportChannelProvider(ManagedChannel emulatorChannel) {
            return GcpClientsExample.channelProvider(emulatorChannel);
        }

        @Bean
        CredentialsProvider credentialsProvider() {
            return NoCredentialsProvider.create();
        }
    }
}
