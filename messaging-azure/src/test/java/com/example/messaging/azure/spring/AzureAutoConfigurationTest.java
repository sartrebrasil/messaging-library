package com.example.messaging.azure.spring;

import com.example.messaging.spring.MessagingAutoConfiguration;
import com.example.messaging.spring.MessagingDestinations;
import org.junit.jupiter.api.Test;
import org.springframework.boot.autoconfigure.AutoConfigurations;
import org.springframework.boot.test.context.runner.ApplicationContextRunner;

import static org.assertj.core.api.Assertions.assertThat;

/** Service Bus pelo starter. Sem rede: os clientes do SDK só conectam no primeiro uso. */
class AzureAutoConfigurationTest {

    @Test
    void azureDeadLetterQueueIsReceiveOnly() {
        ApplicationContextRunner azure = new ApplicationContextRunner()
                .withConfiguration(AutoConfigurations.of(MessagingAutoConfiguration.class))
                .withPropertyValues("messaging.providers.azure.type=azure",
                        "messaging.providers.azure.connection-string=Endpoint=sb://localhost;SharedAccessKeyName=k;"
                                + "SharedAccessKey=v;UseDevelopmentEmulator=true;");

        azure.withPropertyValues(
                        "messaging.destinations.pedidos-dlq.provider=azure",
                        "messaging.destinations.pedidos-dlq.queue=pedidos",
                        "messaging.destinations.pedidos-dlq.sessions=true",
                        "messaging.destinations.pedidos-dlq.dead-letter-queue=true",
                        "messaging.destinations.eventos-dlq.provider=azure",
                        "messaging.destinations.eventos-dlq.topic=eventos",
                        "messaging.destinations.eventos-dlq.subscription=faturamento",
                        "messaging.destinations.eventos-dlq.dead-letter-queue=true")
                .run(context -> {
                    MessagingDestinations destinations = context.getBean(MessagingDestinations.class);
                    // a DLQ não tem sessions: receiver comum, mesmo com sessions na entidade
                    assertThat(destinations.receiver("pedidos-dlq").capabilities().orderedDelivery()).isFalse();
                    assertThat(destinations.receiver("eventos-dlq").capabilities().nativeDeadLetter()).isTrue();
                    org.assertj.core.api.Assertions.assertThatThrownBy(() -> destinations.sender("pedidos-dlq"))
                            .isInstanceOf(IllegalArgumentException.class).hasMessageContaining("não envia");
                });
    }
}
