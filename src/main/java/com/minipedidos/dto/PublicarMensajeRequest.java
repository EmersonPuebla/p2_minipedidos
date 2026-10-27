package com.minipedidos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

public record PublicarMensajeRequest(
        @NotBlank(message = "El exchange es obligatorio")
        String exchange,

        @NotBlank(message = "La routing key es obligatoria")
        String routingKey,

        @NotBlank(message = "El mensaje es obligatorio")
        @Size(max = 10000, message = "El mensaje no puede superar los 10000 caracteres")
        String mensaje
) {
}
