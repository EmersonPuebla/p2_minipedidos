package com.minipedidos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record CrearExchangeRequest(
        @NotBlank(message = "El nombre es obligatorio")
        @Pattern(regexp = "[A-Za-z0-9._-]{1,120}", message = "Nombre de exchange invalido")
        String nombre,

        @NotBlank(message = "El tipo es obligatorio")
        @Pattern(regexp = "direct|fanout|topic|headers", message = "Tipo de exchange no soportado")
        String tipo,

        Boolean durable
) {

    public boolean durableSeguro() {
        return durable == null || durable;
    }
}
