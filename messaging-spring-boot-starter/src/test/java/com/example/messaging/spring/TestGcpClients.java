package com.example.messaging.spring;

import com.google.api.gax.grpc.GrpcTransportChannel;
import com.google.api.gax.rpc.FixedTransportChannelProvider;
import com.google.api.gax.rpc.TransportChannelProvider;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;

/** O canal sem TLS que a aplicação declararia para um emulator do Pub/Sub; nada conecta antes do primeiro uso. */
public final class TestGcpClients {

    private TestGcpClients() {
    }

    public static ManagedChannel plaintext(String hostAndPort) {
        return ManagedChannelBuilder.forTarget(hostAndPort).usePlaintext().build();
    }

    /** Fixo: o SDK não fecha o canal, que é da aplicação. */
    public static TransportChannelProvider provider(ManagedChannel channel) {
        return FixedTransportChannelProvider.create(GrpcTransportChannel.create(channel));
    }
}
