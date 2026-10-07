package com.example.messaging.jms;

import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.MessagingException;
import jakarta.jms.Connection;
import jakarta.jms.ConnectionFactory;
import jakarta.jms.JMSException;
import jakarta.jms.Session;
import org.apache.activemq.artemis.api.core.ActiveMQException;
import org.apache.activemq.artemis.api.core.SimpleString;
import org.apache.activemq.artemis.api.core.client.ClientSession;
import org.apache.activemq.artemis.jms.client.ActiveMQConnection;
import org.apache.activemq.artemis.jms.client.ActiveMQConnectionFactory;
import org.apache.activemq.artemis.jms.client.ActiveMQSession;

/**
 * O que usa classes do ActiveMQ Artemis ({@code artemis-jakarta-client}), carregado só pelo
 * {@link JmsDialect#ARTEMIS}.
 *
 * <p>{@code checkAccess} consulta o broker pela session Core ({@code queueQuery},
 * {@code addressQuery}), que não cria nada; producer, consumer e browser criariam (auto-create).</p>
 */
final class ArtemisSupport {

    static final String WINDOW = "consumerWindowSize";
    static final String RECONNECT = "reconnectAttempts";

    private ArtemisSupport() {
    }

    /**
     * Acrescenta à URL o que ela não define: {@code consumerWindowSize=0} (o receiver exige) e
     * {@code reconnectAttempts=-1}. O padrão do Artemis é 0, e a conexão morreria de vez quando o
     * broker reiniciasse. O startup continua falhando com o broker fora ({@code initialConnectAttempts=1}).
     */
    static ConnectionFactory connectionFactory(String brokerUrl) {
        String url = withDefault(withDefault(brokerUrl, WINDOW, "0"), RECONNECT, "-1");
        return new ActiveMQConnectionFactory(url);
    }

    private static String withDefault(String url, String parameter, String value) {
        return url.contains(parameter) ? url : url + (url.contains("?") ? "&" : "?") + parameter + "=" + value;
    }

    /**
     * Com janela, o broker manda mensagens a mais para o buffer do consumer, presas sem lease e
     * fora do alcance das outras sessions do pool (F8).
     *
     * @throws IllegalArgumentException conexão com {@code consumerWindowSize} diferente de 0
     */
    static void requireNoWindow(Connection connection) {
        if (connection instanceof ActiveMQConnection artemis) {
            int window = artemis.getSessionFactory().getServerLocator().getConsumerWindowSize();
            if (window != 0) {
                throw new IllegalArgumentException("Conexão do Artemis com " + WINDOW + "=" + window
                        + "; o receiver exige " + WINDOW + "=0 na connection factory");
            }
        }
    }

    /**
     * @param name fila, FQQN {@code <endereço>::<fila>} ou, com {@code topic}, endereço multicast
     */
    static void check(Connection connection, String name, boolean topic) {
        try (Session session = connection.createSession(false, Session.AUTO_ACKNOWLEDGE)) {
            if (!(session instanceof ActiveMQSession artemis)) {
                throw new UnsupportedOperationException("checkAccess só com ActiveMQConnection do Artemis (sem pool): "
                        + connection.getClass());
            }
            ClientSession core = artemis.getCoreSession();
            int separator = name.indexOf("::");
            boolean exists;
            if (topic) {
                exists = core.addressQuery(SimpleString.of(name)).isExists();
            } else if (separator >= 0) {
                ClientSession.QueueQuery query = core.queueQuery(SimpleString.of(name.substring(separator + 2)));
                exists = query.isExists() && query.getAddress().toString().equals(name.substring(0, separator));
            } else {
                exists = core.queueQuery(SimpleString.of(name)).isExists();
            }
            if (!exists) {
                throw new DestinationNotFoundException(JmsErrors.PROVIDER, (topic ? "Endereço" : "Fila") + " " + name
                        + " não existe no broker", null);
            }
        } catch (JMSException e) {
            throw JmsErrors.map("checkAccess " + name, e);
        } catch (ActiveMQException e) {
            throw new MessagingException(JmsErrors.PROVIDER, "checkAccess " + name + ": " + e.getMessage(), e, false);
        }
    }
}
