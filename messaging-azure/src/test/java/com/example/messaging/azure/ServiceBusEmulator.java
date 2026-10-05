package com.example.messaging.azure;

import com.azure.core.amqp.AmqpRetryOptions;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import org.testcontainers.azure.ServiceBusEmulatorContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.mssqlserver.MSSQLServerContainer;
import org.testcontainers.utility.MountableFile;

import java.time.Duration;

/**
 * Um Service Bus emulator por JVM, compartilhado pelos contratos. Os entities são fixos
 * ({@code service-bus-config.json}): o emulator só lê a configuração no startup.
 */
final class ServiceBusEmulator {

    /** {@code LockDuration} das filas e da subscription do contrato. */
    static final Duration LEASE = Duration.ofSeconds(5);
    /** O emulator fixa 256 KB. */
    static final long MAX_MESSAGE_BYTES = ServiceBusMessageSender.STANDARD_MAX_MESSAGE_BYTES;

    private static ServiceBusClientBuilder builder;
    private static RuntimeException startFailure;

    private ServiceBusEmulator() {
    }

    static synchronized ServiceBusClientBuilder builder() {
        // falha no startup vale para a JVM toda: sem isso, cada teste sobe um SQL Server novo
        if (startFailure != null) {
            throw startFailure;
        }
        if (builder == null) {
            try {
                start();
            } catch (RuntimeException e) {
                startFailure = e;
                throw e;
            }
        }
        return builder;
    }

    private static void start() {
        {
            Network network = Network.newNetwork();
            MSSQLServerContainer sql = new MSSQLServerContainer("mcr.microsoft.com/mssql/server:2022-CU14-ubuntu-22.04")
                    .acceptLicense()
                    .withPassword("Contract(!)Passw0rd")
                    .withNetwork(network);
            ServiceBusEmulatorContainer emulator =
                    new ServiceBusEmulatorContainer("mcr.microsoft.com/azure-messaging/servicebus-emulator:2.0.0")
                            .acceptLicense()
                            .withConfig(MountableFile.forClasspathResource("/service-bus-config.json"))
                            .withNetwork(network)
                            .withMsSqlServerContainer(sql);
            emulator.start();
            // tryTimeout curto: sem conexão, cada chamada falha em segundos, não nos 245 s do padrão.
            // Também é o tempo que acceptNextSession espera quando não há session.
            builder = new ServiceBusClientBuilder().connectionString(emulator.getConnectionString())
                    .retryOptions(new AmqpRetryOptions().setTryTimeout(Duration.ofSeconds(20)).setMaxRetries(1));
        }
    }
}
