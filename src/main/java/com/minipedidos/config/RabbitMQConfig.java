package com.minipedidos.config;

import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.amqp.support.converter.JacksonJsonMessageConverter;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

/**
 * Constantes y beans que NO cambian en tiempo de ejecucion.
 *
 * La cola principal, el DLX y la DLQ se declaran desde
 * {@code TopologiaService} porque el formulario permite cambiar sus nombres.
 * El exchange principal y la cola de reintentos si son fijos: la cola de
 * reintentos necesita un destino estable al cual devolver los mensajes
 * expirados.
 */
@Configuration
public class RabbitMQConfig {

    // Topologia principal fija
    public static final String EXCHANGE_PEDIDOS = "pedidos.exchange";
    public static final String ROUTING_KEY = "pedidos.nuevo";

    // Topologia dinamica por defecto (el formulario puede cambiarla)
    public static final String COLA_PEDIDOS_DEFAULT = "pedidos.cola";
    public static final String DLX_PEDIDOS_DEFAULT = "pedidos.dlx";
    public static final String ROUTING_KEY_ERROR_DEFAULT = "pedidos.error";
    public static final String DLQ_PEDIDOS_DEFAULT = "pedidos.dlq";

    // Cola de reintentos con TTL + dead-lettering de vuelta al exchange principal
    public static final String EXCHANGE_REINTENTOS = "pedidos.retry.exchange";
    public static final String COLA_REINTENTOS = "pedidos.retry.cola";
    public static final String ROUTING_KEY_REINTENTOS = "pedidos.retry";

    public static final String HEADER_REINTENTOS = "x-retry-count";
    public static final String HEADER_ULTIMO_ERROR = "x-ultimo-error";
    public static final String HEADER_INTENTOS = "x-intentos";
    public static final String HEADER_ORIGEN = "x-origen";

    /** Id del contenedor de @RabbitListener para poder encenderlo/apagarlo. */
    public static final String CONSUMIDOR_ID = "pedidoConsumer";

    @Bean
    public JacksonJsonMessageConverter jacksonMessageConverter() {
        // El productor agrega __TypeId__ con el nombre de la clase. Sin esta
        // lista, Spring AMQP rechaza el mensaje al deserializarlo.
        return new JacksonJsonMessageConverter(
                "com.minipedidos.model",
                "java.util",
                "java.lang"
        );
    }

    // ============================================================
    // Topologia por defecto declarada como beans.
    // RabbitAdmin la crea al conectar. TopologiaService la vuelve a
    // declarar de forma idempotente y permite cambiarla en caliente.
    // ============================================================

    @Bean
    public DirectExchange pedidosExchange() {
        return exchangePrincipal();
    }

    @Bean
    public DirectExchange pedidosDlx() {
        return new DirectExchange(DLX_PEDIDOS_DEFAULT, true, false);
    }

    @Bean
    public Queue pedidosQueue() {
        return QueueBuilder.durable(COLA_PEDIDOS_DEFAULT)
                .deadLetterExchange(DLX_PEDIDOS_DEFAULT)
                .deadLetterRoutingKey(ROUTING_KEY_ERROR_DEFAULT)
                .build();
    }

    @Bean
    public Queue pedidosDlq() {
        return QueueBuilder.durable(DLQ_PEDIDOS_DEFAULT).build();
    }

    @Bean
    public Binding pedidosBinding(
            @Qualifier("pedidosQueue") Queue pedidosQueue,
            @Qualifier("pedidosExchange") DirectExchange pedidosExchange) {

        return BindingBuilder.bind(pedidosQueue)
                .to(pedidosExchange)
                .with(ROUTING_KEY);
    }

    @Bean
    public Binding pedidosDlqBinding(
            @Qualifier("pedidosDlq") Queue pedidosDlq,
            @Qualifier("pedidosDlx") DirectExchange pedidosDlx) {

        return BindingBuilder.bind(pedidosDlq)
                .to(pedidosDlx)
                .with(ROUTING_KEY_ERROR_DEFAULT);
    }

    @Bean
    public DirectExchange pedidosRetryExchange() {
        return exchangeReintentos();
    }

    @Bean
    public Queue pedidosRetryQueue() {
        return colaReintentos();
    }

    @Bean
    public Binding pedidosRetryBinding(
            @Qualifier("pedidosRetryQueue") Queue pedidosRetryQueue,
            @Qualifier("pedidosRetryExchange") DirectExchange pedidosRetryExchange) {

        return BindingBuilder.bind(pedidosRetryQueue)
                .to(pedidosRetryExchange)
                .with(ROUTING_KEY_REINTENTOS);
    }

    public static Queue colaReintentos() {
        return QueueBuilder.durable(COLA_REINTENTOS)
                .deadLetterExchange(EXCHANGE_PEDIDOS)
                .deadLetterRoutingKey(ROUTING_KEY)
                .build();
    }

    public static DirectExchange exchangePrincipal() {
        return ExchangeBuilder.directExchange(EXCHANGE_PEDIDOS)
                .durable(true)
                .build();
    }

    public static DirectExchange exchangeReintentos() {
        return ExchangeBuilder.directExchange(EXCHANGE_REINTENTOS)
                .durable(true)
                .build();
    }
}
