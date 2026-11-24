package com.example.messaging.loadtest;

import com.example.messaging.MessageReceiver;
import com.example.messaging.MessageSender;
import com.example.messaging.aws.SqsMessageReceiver;
import com.example.messaging.aws.SqsMessageSender;
import com.example.messaging.inbox.RetryQueue;
import com.example.messaging.jms.JmsDialect;
import com.example.messaging.jms.JmsMessageReceiver;
import com.example.messaging.jms.JmsMessageSender;
import com.example.messaging.memory.InMemoryMessaging;
import com.example.messaging.memory.InMemoryMessaging.QueueOptions;
import com.example.messaging.rabbitmq.RabbitMessageReceiver;
import com.example.messaging.rabbitmq.RabbitMessageSender;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.ConnectionFactory;
import jakarta.jms.Connection;
import jakarta.jms.Session;
import org.apache.activemq.ActiveMQConnectionFactory;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.wait.strategy.Wait;
import software.amazon.awssdk.auth.credentials.AwsBasicCredentials;
import software.amazon.awssdk.auth.credentials.StaticCredentialsProvider;
import software.amazon.awssdk.http.apache.ApacheHttpClient;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.QueueAttributeName;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

/**
 * Um broker por execução. Para somar um broker novo: implemente esta interface e registre em {@link #create}.
 */
interface BrokerFixture extends AutoCloseable {

    /**
     * O lado do broker de um consumer. {@code senders} e {@code receivers} devolvem instância nova a cada chamada
     * (um sender por produtor, um receiver por worker).
     */
    record Endpoints(Supplier<MessageSender> senders, Supplier<MessageReceiver> receivers, MessageSender failed,
                     MessageSender deadLetter, List<RetryQueue> retryQueues, Duration lease) {
    }

    /** Cria as filas {@code <name>-main|failed|dlq|retry} (de forma idempotente) e devolve o acesso a elas. */
    Endpoints endpoints(String name) throws Exception;

    @Override
    void close() throws Exception;

    static BrokerFixture create(String broker) throws Exception {
        return switch (broker) {
            case "inmemory" -> new InMemory();
            case "rabbit" -> new Rabbit();
            case "sqs" -> new Sqs();
            case "jms-classic" -> new JmsClassic();
            default -> throw new IllegalArgumentException("broker desconhecido: " + broker
                    + " (válidos: inmemory, rabbit, sqs, jms-classic)");
        };
    }

    /** Linha de base: sem rede; isola o custo do próprio listener. */
    final class InMemory implements BrokerFixture {

        private final InMemoryMessaging messaging = new InMemoryMessaging();

        @Override
        public Endpoints endpoints(String name) {
            Duration lease = Duration.ofSeconds(30);
            for (String q : List.of("main", "failed", "dlq", "retry")) {
                messaging.createQueue(name + "-" + q, QueueOptions.defaults().withLease(lease));
            }
            return new Endpoints(() -> messaging.queueSender(name + "-main"), () -> messaging.receiver(name + "-main"),
                    messaging.queueSender(name + "-failed"), messaging.queueSender(name + "-dlq"),
                    List.of(new RetryQueue(Duration.ofSeconds(1), messaging.queueSender(name + "-retry"))), lease);
        }

        @Override
        public void close() {
        }
    }

    /** RabbitMQ 4.1 (a imagem dos testes do adapter); filas quorum, como o contrato da inbox. */
    final class Rabbit implements BrokerFixture {

        private final GenericContainer<?> container = new GenericContainer<>("rabbitmq:4.1-management-alpine")
                .withExposedPorts(5672)
                .waitingFor(Wait.forLogMessage(".*Server startup complete.*", 1));
        private final com.rabbitmq.client.Connection connection;

        Rabbit() throws Exception {
            container.start();
            ConnectionFactory factory = new ConnectionFactory();
            factory.setHost(container.getHost());
            factory.setPort(container.getMappedPort(5672));
            connection = factory.newConnection();
        }

        @Override
        public Endpoints endpoints(String name) throws Exception {
            Duration lease = Duration.ofSeconds(30);
            try (Channel channel = connection.createChannel()) {
                for (String q : List.of("main", "failed", "dlq")) {
                    channel.queueDeclare(name + "-" + q, true, false, false, Map.of("x-queue-type", "quorum"));
                }
                // banda do retry-queues: a infraestrutura devolve ao main depois do TTL (a lib não verifica isso)
                channel.queueDeclare(name + "-retry", true, false, false, Map.of("x-queue-type", "quorum",
                        "x-message-ttl", 1000, "x-dead-letter-exchange", "",
                        "x-dead-letter-routing-key", name + "-main"));
            }
            long max = RabbitMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
            return new Endpoints(() -> RabbitMessageSender.forQueue(connection, name + "-main", max),
                    () -> new RabbitMessageReceiver(connection, name + "-main", lease,
                            RabbitMessageSender.forQueue(connection, name + "-dlq", max)),
                    RabbitMessageSender.forQueue(connection, name + "-failed", max),
                    RabbitMessageSender.forQueue(connection, name + "-dlq", max),
                    List.of(new RetryQueue(Duration.ofSeconds(1),
                            RabbitMessageSender.forQueue(connection, name + "-retry", max))), lease);
        }

        @Override
        public void close() throws Exception {
            connection.close();
            container.stop();
        }
    }

    /** SQS no LocalStack (4.14.0, a última imagem que roda sem token); filas standard. */
    final class Sqs implements BrokerFixture {

        private final GenericContainer<?> container = new GenericContainer<>("localstack/localstack:4.14.0")
                .withEnv("SERVICES", "sqs")
                .withExposedPorts(4566)
                .waitingFor(Wait.forLogMessage(".*Ready\\.\\n", 1));
        private final SqsClient sqs;

        Sqs() {
            container.start();
            URI endpoint = URI.create("http://" + container.getHost() + ":" + container.getMappedPort(4566));
            sqs = SqsClient.builder().region(Region.US_EAST_1).endpointOverride(endpoint)
                    .credentialsProvider(StaticCredentialsProvider.create(AwsBasicCredentials.create("test", "test")))
                    // read timeout acima dos 20 s do long poll
                    .httpClientBuilder(ApacheHttpClient.builder().socketTimeout(Duration.ofSeconds(30))).build();
        }

        private String queue(String name) {
            return sqs.createQueue(b -> b.queueName(name)
                    .attributes(Map.of(QueueAttributeName.VISIBILITY_TIMEOUT, "30"))).queueUrl();
        }

        @Override
        public Endpoints endpoints(String name) {
            String main = queue(name + "-main");
            String failed = queue(name + "-failed");
            String dlq = queue(name + "-dlq");
            String retry = queue(name + "-retry");
            return new Endpoints(() -> new SqsMessageSender(sqs, main), () -> new SqsMessageReceiver(sqs, main),
                    new SqsMessageSender(sqs, failed), new SqsMessageSender(sqs, dlq),
                    List.of(new RetryQueue(Duration.ofSeconds(1), new SqsMessageSender(sqs, retry))),
                    Duration.ofSeconds(30));
        }

        @Override
        public void close() {
            sqs.close();
            container.stop();
        }
    }

    /** ActiveMQ Classic com {@code schedulerSupport} (atraso do nível 2 por cópia agendada), como o contrato. */
    final class JmsClassic implements BrokerFixture {

        private static final JmsDialect DIALECT = JmsDialect.ACTIVEMQ_CLASSIC;

        private final GenericContainer<?> container = new GenericContainer<>("apache/activemq-classic:6.2.0")
                .withExposedPorts(61616)
                .withCreateContainerCmdModifier(cmd -> cmd.withEntrypoint("sh"))
                .withCommand("-c", "sed -i 's/brokerName=\"localhost\"/brokerName=\"localhost\" schedulerSupport=\"true\"/'"
                        + " /opt/apache-activemq/conf/activemq.xml && exec /opt/apache-activemq/bin/activemq console")
                .waitingFor(Wait.forLogMessage(".*Apache ActiveMQ .* started.*", 1));
        private final Connection connection;

        JmsClassic() throws Exception {
            container.start();
            connection = new ActiveMQConnectionFactory(
                    "tcp://" + container.getHost() + ":" + container.getMappedPort(61616))
                    .createConnection("admin", "admin");
            connection.start();
        }

        /** Auto-create do Classic: abrir e fechar um consumer cria a fila. */
        private String queue(String name) throws Exception {
            Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE);
            session.createConsumer(session.createQueue(name)).close();
            session.close();
            return name;
        }

        @Override
        public Endpoints endpoints(String name) throws Exception {
            Duration lease = Duration.ofSeconds(30);
            String main = queue(name + "-main");
            String failed = queue(name + "-failed");
            String dlq = queue(name + "-dlq");
            String retry = queue(name + "-retry");
            long max = JmsMessageSender.DEFAULT_MAX_MESSAGE_BYTES;
            return new Endpoints(() -> JmsMessageSender.forQueue(connection, DIALECT, main, max),
                    () -> new JmsMessageReceiver(connection, DIALECT, main, lease, 10,
                            JmsMessageReceiver.Redelivery.SCHEDULED, null),
                    JmsMessageSender.forQueue(connection, DIALECT, failed, max),
                    JmsMessageSender.forQueue(connection, DIALECT, dlq, max),
                    List.of(new RetryQueue(Duration.ofSeconds(1),
                            JmsMessageSender.forQueue(connection, DIALECT, retry, max))), lease);
        }

        @Override
        public void close() throws Exception {
            connection.close();
            container.stop();
        }
    }
}
