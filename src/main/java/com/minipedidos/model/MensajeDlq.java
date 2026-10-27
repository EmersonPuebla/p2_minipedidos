package com.minipedidos.model;

import java.util.Map;

/**
 * Vista de un mensaje que esta en la Dead Letter Queue.
 */
public record MensajeDlq(
        String id,
        String messageId,
        Long pedidoId,
        String cliente,
        String producto,
        Integer cantidad,
        String motivo,
        String colaOrigen,
        String exchange,
        String routingKey,
        Integer reintentos,
        String payload,
        Map<String, Object> headers,
        Long fecha
) {
}
