package com.minipedidos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CrearBindingRequest(
        @NotBlank(message = "El exchange es obligatorio")
        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de exchange invalido")
        String exchange,

        @NotBlank(message = "La cola es obligatoria")
        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de cola invalido")
        String cola,

        @NotBlank(message = "La routing key es obligatoria")
        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Routing key invalida")
        String routingKey
) {
}
