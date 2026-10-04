package com.example.messaging.aws;

import com.example.messaging.AccessDeniedException;
import com.example.messaging.DestinationNotFoundException;
import com.example.messaging.LeaseExpiredException;
import com.example.messaging.MessageTooLargeException;
import com.example.messaging.MessagingException;
import com.example.messaging.ThrottledException;
import software.amazon.awssdk.awscore.exception.AwsServiceException;
import software.amazon.awssdk.core.exception.SdkException;

import java.util.Locale;
import java.util.Set;

/** Converte erros do SQS e do SNS nas exceções da lib (ADR-0007). */
final class AwsErrors {

    static final String PROVIDER = "aws";

    private static final Set<String> NOT_FOUND = Set.of("QueueDoesNotExist", "AWS.SimpleQueueService.NonExistentQueue",
            "NotFound", "NotFoundException");
    private static final Set<String> ACCESS_DENIED = Set.of("AccessDenied", "AccessDeniedException", "AuthorizationError",
            "InvalidClientTokenId", "SignatureDoesNotMatch", "InvalidSecurity", "KMSAccessDenied");
    private static final Set<String> THROTTLED = Set.of("RequestThrottled", "Throttling", "ThrottlingException",
            "ThrottledException", "KMSThrottling", "KMSThrottlingException");
    private static final Set<String> LEASE_LOST = Set.of("ReceiptHandleIsInvalid", "AWS.SimpleQueueService.ReceiptHandleIsInvalid",
            "MessageNotInflight", "AWS.SimpleQueueService.MessageNotInflight");
    private static final Set<String> TOO_LARGE = Set.of("BatchRequestTooLong", "AWS.SimpleQueueService.BatchRequestTooLong");

    private AwsErrors() {
    }

    static MessagingException map(String operation, SdkException e) {
        if (e instanceof AwsServiceException service) {
            String code = service.awsErrorDetails() == null ? null : service.awsErrorDetails().errorCode();
            String text = service.awsErrorDetails() == null ? e.getMessage() : service.awsErrorDetails().errorMessage();
            return map(operation, code, text, service.statusCode(), e);
        }
        return new MessagingException(PROVIDER, operation + ": " + e.getMessage(), e, e.retryable());
    }

    /** Também usado para as falhas por entrada de um lote, que não vêm como exceção. */
    static MessagingException map(String operation, String code, String text, int status, Throwable cause) {
        String message = operation + ": " + code + " " + text;
        if (code == null) {
            return new MessagingException(PROVIDER, message, cause, status >= 500);
        }
        if (NOT_FOUND.contains(code)) {
            return new DestinationNotFoundException(PROVIDER, message, cause);
        }
        if (ACCESS_DENIED.contains(code)) {
            return new AccessDeniedException(PROVIDER, message, cause);
        }
        if (THROTTLED.contains(code)) {
            return new ThrottledException(PROVIDER, message, cause);
        }
        if (LEASE_LOST.contains(code) || isExpiredReceiptHandle(code, text)) {
            return new LeaseExpiredException(PROVIDER, message, cause);
        }
        if (TOO_LARGE.contains(code)) {
            return new MessageTooLargeException(PROVIDER, message, cause);
        }
        return new MessagingException(PROVIDER, message, cause, status >= 500);
    }

    /** O SQS costuma responder handle vencido como {@code InvalidParameterValue} "... receipt handle has expired". */
    private static boolean isExpiredReceiptHandle(String code, String text) {
        return code.endsWith("InvalidParameterValue") && text != null
                && text.toLowerCase(Locale.ROOT).contains("receipt handle");
    }
}
