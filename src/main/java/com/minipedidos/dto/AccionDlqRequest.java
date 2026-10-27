package com.minipedidos.dto;

import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Pattern;

public record AccionDlqRequest(
        @NotBlank(message = "Indica el ID del mensaje")
        String mensajeDlq,

        @NotBlank(message = "La accion es obligatoria")
        @Pattern(regexp = "inspeccionar|reprocesar|descartar",
                message = "Accion no soportada")
        String accionDlq
) {
}
