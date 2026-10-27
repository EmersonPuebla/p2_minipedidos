package com.minipedidos.model;

import java.util.Map;

/**
 * Evento emitido por el backend y enviado al navegador mediante SSE.
 */
public record Evento(
        long id,
        long fecha,
        String tipo,
        String mensaje,
        Map<String, Object> datos
) {
}
