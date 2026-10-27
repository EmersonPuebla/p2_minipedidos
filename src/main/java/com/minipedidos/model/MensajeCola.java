package com.minipedidos.model;

/**
 * Vista de un mensaje que esta esperando en una cola.
 */
public record MensajeCola(
        String id,
        String messageId,
        Long pedidoId,
        String cliente,
        String producto,
        Integer cantidad,
        String estado,
        Integer reintentos,
        String payload
) {
}
