package com.minipedidos.service;

import tools.jackson.databind.ObjectMapper;
import com.minipedidos.model.Evento;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.MediaType;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.web.servlet.mvc.method.annotation.SseEmitter;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Bus de eventos en memoria + stream SSE para el monitor en vivo del
 * formulario. No pretende ser un bus de eventos de produccion: es una
 * herramienta didactica para ver ACK, NACK, reintentos y DLQ en tiempo real.
 */
@Service
public class EventoService {

    private static final Logger log = LoggerFactory.getLogger(EventoService.class);
    private static final int MAX_HISTORIAL = 250;

    private final ObjectMapper objectMapper;
    private final List<SseEmitter> emisores = new CopyOnWriteArrayList<>();
    private final Deque<Evento> historial = new ArrayDeque<>();
    private final AtomicLong secuencia = new AtomicLong();

    public EventoService(ObjectMapper objectMapper) {
        this.objectMapper = objectMapper;
    }

    public SseEmitter suscribir() {
        SseEmitter emisor = new SseEmitter(0L);
        emisores.add(emisor);
        emisor.onCompletion(() -> emisores.remove(emisor));
        emisor.onTimeout(() -> emisores.remove(emisor));
        emisor.onError(error -> emisores.remove(emisor));
        enviar(emisor, new Evento(
                secuencia.incrementAndGet(),
                System.currentTimeMillis(),
                "CONECTADO",
                "Monitor en vivo conectado",
                Map.of("emisores", emisores.size())
        ));
        return emisor;
    }

    public void publicar(String tipo, String mensaje, Map<String, Object> datos) {
        Evento evento = new Evento(
                secuencia.incrementAndGet(),
                System.currentTimeMillis(),
                tipo,
                mensaje,
                datos == null ? Map.of() : datos
        );

        synchronized (historial) {
            historial.addFirst(evento);
            while (historial.size() > MAX_HISTORIAL) {
                historial.removeLast();
            }
        }

        log.info("[EVENTO] {} - {}", tipo, mensaje);
        for (SseEmitter emisor : emisores) {
            enviar(emisor, evento);
        }
    }

    public List<Evento> historial() {
        synchronized (historial) {
            return new ArrayList<>(historial);
        }
    }

    @Scheduled(fixedRate = 15_000)
    public void latido() {
        for (SseEmitter emisor : emisores) {
            try {
                emisor.send(SseEmitter.event().comment("ping"));
            } catch (IOException | IllegalStateException e) {
                emisores.remove(emisor);
            }
        }
    }

    private void enviar(SseEmitter emisor, Evento evento) {
        try {
            String json = objectMapper.writeValueAsString(evento);
            emisor.send(SseEmitter.event()
                    .id(String.valueOf(evento.id()))
                    .name("evento")
                    .data(json, MediaType.APPLICATION_JSON));
        } catch (Exception e) {
            emisores.remove(emisor);
            log.debug("SSE desconectado: {}", e.getMessage());
        }
    }

    /**
     * Ayuda para construir mapas de datos sin fallar con valores null.
     */
    public static Map<String, Object> datos(Object... pares) {
        Map<String, Object> resultado = new LinkedHashMap<>();
        for (int i = 0; i + 1 < pares.length; i += 2) {
            Object valor = pares[i + 1];
            if (valor != null) {
                resultado.put(String.valueOf(pares[i]), valor);
            }
        }
        return resultado;
    }
}
