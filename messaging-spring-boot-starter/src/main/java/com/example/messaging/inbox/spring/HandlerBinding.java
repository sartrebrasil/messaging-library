package com.example.messaging.inbox.spring;

import com.example.messaging.inbox.HandlerResult;
import com.example.messaging.inbox.InboxHandler;
import com.example.messaging.inbox.InboxMessage;
import com.example.messaging.inbox.InboxResultHandler;
import com.example.messaging.inbox.TypedMessage;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.factory.config.BeanDefinition;
import org.springframework.beans.factory.config.ConfigurableListableBeanFactory;
import org.springframework.beans.factory.support.RootBeanDefinition;
import org.springframework.core.MethodIntrospector;
import org.springframework.core.ResolvableType;
import org.springframework.core.annotation.AnnotatedElementUtils;
import org.springframework.util.ClassUtils;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.lang.reflect.TypeVariable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;

/**
 * Finds the handler of each consumer (ADR-0008, plan): a functional bean named after the consumer (exact or
 * camelCase: {@code billing-orders} or {@code billingOrders}), or an {@link InboxListener} method. Exactly one per
 * consumer; every problem is reported at once.
 */
final class HandlerBinding {

    /** A consumer's handler and the payload type it wants. */
    record Bound(String description, Type payloadType, Supplier<InboxResultHandler<Object>> handler) {
    }

    private HandlerBinding() {
    }

    /** @throws IllegalStateException listing every problem */
    static Map<String, Bound> bind(Set<String> consumers, ConfigurableListableBeanFactory beans) {
        List<String> problems = new ArrayList<>();
        Map<String, List<Bound>> found = new HashMap<>();
        functional(consumers, beans, found, problems);
        annotated(consumers, beans, found, problems);
        Map<String, Bound> bound = new HashMap<>();
        for (String consumer : consumers) {
            List<Bound> candidates = found.getOrDefault(consumer, List.of());
            if (candidates.isEmpty()) {
                String names = camel(consumer).equals(consumer) ? "'" + consumer + "'"
                        : "'" + consumer + "' or '" + camel(consumer) + "'";
                problems.add(InboxConfig.RECEIVERS + consumer + " has no handler: declare a bean named " + names
                        + " of type InboxHandler/InboxResultHandler, or an @InboxListener");
            } else if (candidates.size() > 1) {
                problems.add(InboxConfig.RECEIVERS + consumer + " has more than one handler: "
                        + candidates.stream().map(Bound::description).toList());
            } else {
                bound.put(consumer, candidates.getFirst());
            }
        }
        if (!problems.isEmpty()) {
            throw new IllegalStateException("Invalid inbox handlers:\n - " + String.join("\n - ", problems));
        }
        return bound;
    }

    @SuppressWarnings("unchecked")
    private static void functional(Set<String> consumers, ConfigurableListableBeanFactory beans,
                                   Map<String, List<Bound>> found, List<String> problems) {
        for (Class<?> kind : List.of(InboxHandler.class, InboxResultHandler.class)) {
            for (String name : beans.getBeanNamesForType(kind, true, false)) {
                String consumer = consumerOf(name, consumers);
                if (consumer == null) {
                    continue;
                }
                Type payload = beanGeneric(beans, name, kind);
                if (payload == null) {
                    problems.add("bean '" + name + "': cannot resolve the payload type of " + kind.getSimpleName()
                            + "; declare the @Bean method as " + kind.getSimpleName() + "<YourType>");
                    continue;
                }
                Supplier<InboxResultHandler<Object>> handler = kind == InboxHandler.class
                        ? () -> InboxResultHandler.of((InboxHandler<Object>) beans.getBean(name))
                        : () -> (InboxResultHandler<Object>) beans.getBean(name);
                found.computeIfAbsent(consumer, k -> new ArrayList<>())
                        .add(new Bound("bean '" + name + "'", payload, handler));
            }
        }
    }

    private static void annotated(Set<String> consumers, ConfigurableListableBeanFactory beans,
                                  Map<String, List<Bound>> found, List<String> problems) {
        for (String name : beans.getBeanDefinitionNames()) {
            Class<?> type = beans.getType(name, false);
            if (type == null) {
                continue;
            }
            Class<?> user = ClassUtils.getUserClass(type);
            Map<Method, InboxListener> methods = MethodIntrospector.selectMethods(user,
                    (MethodIntrospector.MetadataLookup<InboxListener>) m ->
                            AnnotatedElementUtils.findMergedAnnotation(m, InboxListener.class));
            methods.forEach((method, listener) -> {
                String where = user.getName() + "#" + method.getName();
                if (!consumers.contains(listener.consumer())) {
                    problems.add("@InboxListener " + where + ": consumer '" + listener.consumer()
                            + "' does not exist in messaging.receivers");
                    return;
                }
                Bound bound = listenerMethod(name, method, type, where, beans, problems);
                if (bound != null) {
                    found.computeIfAbsent(listener.consumer(), k -> new ArrayList<>()).add(bound);
                }
            });
        }
    }

    private static Bound listenerMethod(String beanName, Method method, Class<?> beanType, String where,
                                        ConfigurableListableBeanFactory beans, List<String> problems) {
        Class<?> returns = method.getReturnType();
        if (returns != void.class && !HandlerResult.class.isAssignableFrom(returns)) {
            problems.add("@InboxListener " + where + " must return void or HandlerResult");
            return null;
        }
        Type[] params = method.getGenericParameterTypes();
        Shape shape;
        Type payload;
        if (params.length == 1 && raw(params[0]) == TypedMessage.class) {
            shape = Shape.TYPED;
            payload = params[0] instanceof ParameterizedType p ? p.getActualTypeArguments()[0] : null;
        } else if (params.length == 1) {
            shape = Shape.PAYLOAD;
            payload = params[0];
        } else if (params.length == 2 && method.getParameterTypes()[1] == InboxMessage.class) {
            shape = Shape.PAYLOAD_AND_MESSAGE;
            payload = params[0];
        } else {
            problems.add("@InboxListener " + where + ": parameters must be (T), (T, InboxMessage) or (TypedMessage<T>)");
            return null;
        }
        if (payload == null || payload instanceof TypeVariable<?>) {
            problems.add("@InboxListener " + where + ": cannot resolve the payload type");
            return null;
        }
        Method invocable = AopUtils.selectInvocableMethod(method, beanType);
        invocable.setAccessible(true);
        Supplier<InboxResultHandler<Object>> handler = () -> {
            Object bean = beans.getBean(beanName);
            return message -> {
                Object[] args = switch (shape) {
                    case TYPED -> new Object[] {message};
                    case PAYLOAD -> new Object[] {message.payload()};
                    case PAYLOAD_AND_MESSAGE -> new Object[] {message.payload(), message.message()};
                };
                try {
                    return (HandlerResult) invocable.invoke(bean, args);
                } catch (InvocationTargetException e) {
                    // the handler's own exception, so classification sees it (ADR-0014)
                    if (e.getCause() instanceof Exception cause) {
                        throw cause;
                    }
                    throw (Error) e.getCause();
                }
            };
        };
        return new Bound("@InboxListener " + where, payload, handler);
    }

    private enum Shape { PAYLOAD, PAYLOAD_AND_MESSAGE, TYPED }

    private static Type beanGeneric(ConfigurableListableBeanFactory beans, String name, Class<?> kind) {
        BeanDefinition definition = beans.getMergedBeanDefinition(name);
        ResolvableType type = definition instanceof RootBeanDefinition root ? root.getResolvableType()
                : ResolvableType.forClass(beans.getType(name));
        ResolvableType generic = type.as(kind).getGeneric(0);
        return generic.resolve() == null ? null : generic.getType();
    }

    private static String consumerOf(String beanName, Set<String> consumers) {
        if (consumers.contains(beanName)) {
            return beanName;
        }
        return consumers.stream().filter(c -> camel(c).equals(beanName)).findFirst().orElse(null);
    }

    static String camel(String name) {
        StringBuilder out = new StringBuilder();
        boolean upper = false;
        for (char c : name.toCharArray()) {
            if (c == '-' || c == '_' || c == '.') {
                upper = true;
            } else {
                out.append(upper ? Character.toUpperCase(c) : c);
                upper = false;
            }
        }
        return out.toString();
    }

    private static Class<?> raw(Type type) {
        return type instanceof ParameterizedType p ? (Class<?>) p.getRawType() : type instanceof Class<?> c ? c : null;
    }
}
