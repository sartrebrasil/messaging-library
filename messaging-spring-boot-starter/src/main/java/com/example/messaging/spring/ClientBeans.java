package com.example.messaging.spring;

import org.springframework.beans.factory.ListableBeanFactory;

/**
 * Onde a conexão de um provider procura o cliente nativo da aplicação (ADR-0010). Um provider usa, nesta ordem:
 * o bean chamado como ele ({@code aws}) ou {@code <provider><TipoSimples>} ({@code awsSnsClient}, porque um provider
 * AWS tem dois clientes e dois beans não têm o mesmo nome); senão, só quando nenhum outro provider é do mesmo grupo,
 * o bean do tipo, qualquer que seja o nome.
 */
public final class ClientBeans {

    private ClientBeans() {
    }

    /** Para as mensagens de "defina X ou declare um bean": o bean que existe e não casa precisa de um destes nomes. */
    public static String namingHint(String providerName, Class<?> type) {
        return "com mais de um provider do mesmo tipo, o bean precisa se chamar '" + providerName + "' ou '"
                + providerName + type.getSimpleName() + "'";
    }

    /**
     * @param onlyOfItsType nenhum outro provider usa este tipo de cliente
     * @return o bean, ou {@code null} quando o starter deve criar o cliente
     */
    public static <T> T find(ListableBeanFactory beans, Class<T> type, String providerName, boolean onlyOfItsType) {
        for (String name : new String[]{providerName, providerName + type.getSimpleName()}) {
            if (beans.containsBean(name) && beans.isTypeMatch(name, type)) {
                return beans.getBean(name, type);
            }
        }
        return onlyOfItsType ? beans.getBeanProvider(type).getIfAvailable() : null;
    }
}
