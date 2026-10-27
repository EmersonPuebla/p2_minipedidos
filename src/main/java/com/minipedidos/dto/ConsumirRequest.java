package com.minipedidos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

/**
 * Consumo manual de un mensaje puntual de una cola.
 *
 * El campo tipoError es opcional: vacio significa "consumir normal".
 */
public record ConsumirRequest(
        @NotBlank(message = "La cola es obligatoria")
        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de cola invalido")
        String cola,

        @NotBlank(message = "Selecciona un mensaje")
        String mensajeId,

        @Pattern(
                regexp = "^(|rechazado|requeue|timeout|dlq-recuperable)$",
                message = "Accion no soportada"
        )
        String tipoError
) {
}
