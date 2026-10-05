package com.example.messaging.gcp;

import com.google.api.gax.rpc.ApiException;
import com.google.api.gax.rpc.StatusCode;
import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessagingException;
import com.example.messaging.ThrottledException;

import java.util.Map;

/** Converte erros do Pub/Sub nas exceções da lib (ADR-0007). */
final class GcpErrors {

    static final String PROVIDER = "gcp";

    /** Código por ack id que o Pub/Sub devolve com exactly-once quando o lease já venceu. */
    static final String INVALID_ACK_ID = "PERMANENT_FAILURE_INVALID_ACK_ID";

    private GcpErrors() {
    }

    static MessagingException map(String operation, Throwable e) {
        String message = operation + ": " + e.getMessage();
        if (!(e instanceof ApiException api)) {
            return new MessagingException(PROVIDER, message, e, false);
        }
        StatusCode.Code code = api.getStatusCode().getCode();
        return switch (code) {
            case NOT_FOUND -> new DestinationNotFoundException(PROVIDER, message, e);
            case PERMISSION_DENIED, UNAUTHENTICATED -> new AccessDeniedException(PROVIDER, message, e);
            case RESOURCE_EXHAUSTED -> new ThrottledException(PROVIDER, message, e);
            default -> ackFailures(api).containsValue(INVALID_ACK_ID)
                    ? new LeaseExpiredException(PROVIDER, message, e)
                    : new MessagingException(PROVIDER, message, e, api.isRetryable());
        };
    }

    /**
     * Falhas por ack id no {@code ErrorInfo} (exactly-once): ack id → código. Ack ids fora do mapa
     * foram confirmados.
     */
    static Map<String, String> ackFailures(ApiException e) {
        if (e.getErrorDetails() == null || e.getErrorDetails().getErrorInfo() == null) {
            return Map.of();
        }
        return e.getErrorDetails().getErrorInfo().getMetadataMap();
    }
}
