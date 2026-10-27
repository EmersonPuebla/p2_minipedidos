package com.minipedidos.dto;

import jakarta.validation.constraints.Pattern;

/**
 * Creacion de una cola desde el modulo de administracion.
 *
 * Si se indica DLX, routing key de error y DLQ, se crea tambien la topologia
 * de mensajes muertos y se enlaza la DLQ al DLX.
 */
public record CrearColaRequest(
        @jakarta.validation.constraints.NotBlank(message = "El nombre es obligatorio")
        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de cola invalido")
        String nombre,

        Boolean durable,

        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de DLX invalido")
        String dlx,

        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Routing key invalida")
        String routingKeyDlx,

        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de DLQ invalido")
        String dlq
) {

    public boolean durableSeguro() {
        return durable == null || durable;
    }

    public boolean tieneDlx() {
        return (dlx != null && !dlx.isBlank())
                || (routingKeyDlx != null && !routingKeyDlx.isBlank())
                || (dlq != null && !dlq.isBlank());
    }
}
