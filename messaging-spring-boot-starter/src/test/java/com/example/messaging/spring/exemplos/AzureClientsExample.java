package com.example.messaging.spring.exemplos;

import com.azure.core.credential.TokenCredential;
import com.azure.identity.DefaultAzureCredentialBuilder;
import com.azure.messaging.servicebus.ServiceBusClientBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Como a aplicação declara o {@link ServiceBusClientBuilder} que o starter usa com {@code type: azure} (ADR-0011). O
 * starter cria, a partir dele, um sender ou receiver por fila ou subscription; a conexão AMQP é compartilhada.
 *
 * <p>O builder é declarado sem {@code buildClient()}: a aplicação só escolhe como autenticar. Em produção, o Entra ID
 * (identidade gerenciada) pela {@code DefaultAzureCredential}, que exige {@code com.azure:azure-identity}.</p>
 */
public final class AzureClientsExample {

    /** Service Bus Emulator (Azure): chave de desenvolvimento e {@code UseDevelopmentEmulator}. */
    private static final String EMULATOR =
            "Endpoint=sb://localhost;SharedAccessKeyName=k;SharedAccessKey=v;UseDevelopmentEmulator=true;";

    private AzureClientsExample() {
    }

    /** Chave de acesso compartilhada, da connection string do namespace. */
    public static ServiceBusClientBuilder fromConnectionString(String connectionString) {
        return new ServiceBusClientBuilder().connectionString(connectionString);
    }

    /** Entra ID: {@code fullyQualifiedNamespace} é, por exemplo, {@code meu-namespace.servicebus.windows.net}. */
    public static ServiceBusClientBuilder fromNamespace(String fullyQualifiedNamespace, TokenCredential credential) {
        return new ServiceBusClientBuilder().credential(fullyQualifiedNamespace, credential);
    }

    /** Produção: identidade gerenciada (ou o login do ambiente) pela {@code DefaultAzureCredential}. */
    @Configuration(proxyBeanMethods = false)
    public static class Producao {

        @Bean
        ServiceBusClientBuilder serviceBusClientBuilder(@Value("${azure.servicebus.namespace}") String namespace) {
            return AzureClientsExample.fromNamespace(namespace, new DefaultAzureCredentialBuilder().build());
        }
    }

    /** Emulador: a connection string de desenvolvimento do Service Bus Emulator. */
    @Configuration(proxyBeanMethods = false)
    public static class Emulador {

        @Bean
        ServiceBusClientBuilder serviceBusClientBuilder() {
            return AzureClientsExample.fromConnectionString(EMULATOR);
        }
    }
}
