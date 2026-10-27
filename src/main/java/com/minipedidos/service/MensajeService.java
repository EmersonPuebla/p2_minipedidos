package com.minipedidos.service;

import com.minipedidos.producer.MensajeProducer;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

@Service
public class MensajeService {

    private final MensajeProducer producer;

    public MensajeService(MensajeProducer producer) {
        this.producer = producer;
    }

    public Map<String, Object> publicar(String exchange, String routingKey, String mensaje) {
        String messageId = producer.publicar(exchange, routingKey, mensaje);

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("mensaje", "Mensaje publicado correctamente");
        respuesta.put("messageId", messageId);
        respuesta.put("exchange", exchange);
        respuesta.put("routingKey", routingKey);
        return respuesta;
    }
}
