package com.minipedidos.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record PoliticaReintentosRequest(
        @NotNull(message = "Indica el maximo de reintentos")
        @Min(value = 0, message = "El minimo es 0")
        @Max(value = 10, message = "El maximo es 10")
        Integer maxReintentos,

        @NotNull(message = "Indica el intervalo")
        @Min(value = 1, message = "El intervalo minimo es 1 segundo")
        @Max(value = 300, message = "El intervalo maximo es 300 segundos")
        Integer intervalo
) {
}
