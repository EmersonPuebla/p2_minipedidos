package com.minipedidos.service;

import com.minipedidos.model.PoliticaReintentos;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.util.concurrent.atomic.AtomicReference;

/**
 * Politica de reintentos en memoria, modificable desde el formulario.
 *
 * La cola de reintentos usa TTL por mensaje
 * ({@code expiration = intervaloSegundos * 1000}). RabbitMQ expira los mensajes
 * desde la cabeza de la cola, por lo que intervalos muy distintos en una misma
 * cola pueden retrasar mensajes posteriores. Para la demo es suficiente y
 * evita bloquear el hilo del consumidor con {@code Thread.sleep}.
 */
@Service
public class PoliticaReintentosService {

    private final AtomicReference<PoliticaReintentos> politica;

    public PoliticaReintentosService(
            @Value("${demo.reintentos.max:3}") int maxReintentos,
            @Value("${demo.reintentos.intervalo-segundos:5}") int intervaloSegundos) {

        this.politica = new AtomicReference<>(
                new PoliticaReintentos(
                        limitar(maxReintentos, 0, 10),
                        limitar(intervaloSegundos, 1, 300)
                )
        );
    }

    public PoliticaReintentos getPolitica() {
        return politica.get();
    }

    public PoliticaReintentos actualizar(int maxReintentos, int intervaloSegundos) {
        if (maxReintentos < 0 || maxReintentos > 10) {
            throw new IllegalArgumentException("El maximo de reintentos debe estar entre 0 y 10");
        }
        if (intervaloSegundos < 1 || intervaloSegundos > 300) {
            throw new IllegalArgumentException("El intervalo debe estar entre 1 y 300 segundos");
        }
        PoliticaReintentos nueva = new PoliticaReintentos(maxReintentos, intervaloSegundos);
        politica.set(nueva);
        return nueva;
    }

    private static int limitar(int valor, int min, int max) {
        return Math.max(min, Math.min(max, valor));
    }
}
