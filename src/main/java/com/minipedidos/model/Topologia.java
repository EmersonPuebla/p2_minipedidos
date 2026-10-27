package com.minipedidos.model;

import com.minipedidos.config.RabbitMQConfig;

/**
 * Nombres de la topologia configurable desde el formulario.
 *
 * El exchange principal ({@code pedidos.exchange}) y la routing key principal
 * ({@code pedidos.nuevo}) se mantienen fijos para que la cola de reintentos
 * pueda devolver los mensajes a la cola principal activa.
 */
public record Topologia(
        String colaPrincipal,
        String exchangeDlx,
        String routingKeyDlx,
        String colaDlq
) {

    public static Topologia porDefecto() {
        return new Topologia(
                RabbitMQConfig.COLA_PEDIDOS_DEFAULT,
                RabbitMQConfig.DLX_PEDIDOS_DEFAULT,
                RabbitMQConfig.ROUTING_KEY_ERROR_DEFAULT,
                RabbitMQConfig.DLQ_PEDIDOS_DEFAULT
        );
    }
}
