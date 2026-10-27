package com.minipedidos.model;

/**
 * Politica de reintentos controlada desde el formulario web.
 *
 * @param maxReintentos    reintentos adicionales despues del primer intento
 * @param intervaloSegundos espera entre intentos
 */
public record PoliticaReintentos(int maxReintentos, int intervaloSegundos) {

    public static PoliticaReintentos porDefecto() {
        return new PoliticaReintentos(3, 5);
    }

    public long intervaloMs() {
        return intervaloSegundos * 1000L;
    }
}
