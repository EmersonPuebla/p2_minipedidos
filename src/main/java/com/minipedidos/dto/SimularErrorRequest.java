package com.minipedidos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record SimularErrorRequest(
        @NotBlank(message = "Indica un ID de pedido o un nombre de cliente")
        String cliente,

        @NotBlank(message = "El tipo de error es obligatorio")
        @Pattern(regexp = "rechazado|requeue|timeout|fallo|temporal|error|dlq-recuperable",
                message = "Tipo de error no soportado")
        String tipoError
) {
}
