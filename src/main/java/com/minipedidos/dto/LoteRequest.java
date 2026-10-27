package com.minipedidos.dto;

import jakarta.validation.constraints.Max;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;

public record LoteRequest(
        @NotNull(message = "Indica la cantidad de pedidos")
        @Min(value = 1, message = "El minimo es 1 pedido")
        @Max(value = 20, message = "El maximo es 20 pedidos")
        Integer cantidad
) {
}
