package com.minipedidos.service;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.dto.ConsumirRequest;
import org.springframework.amqp.core.MessageListenerContainer;
import org.springframework.amqp.rabbit.listener.RabbitListenerEndpointRegistry;
import org.springframework.stereotype.Service;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Control del consumidor principal: encender, apagar y consumir un mensaje
 * puntual de forma manual.
 */
@Service
public class ConsumidorService {

    private final RabbitListenerEndpointRegistry registry;
    private final ColaService colaService;

    public ConsumidorService(
            RabbitListenerEndpointRegistry registry,
            ColaService colaService) {

        this.registry = registry;
        this.colaService = colaService;
    }

    public Map<String, Object> estado() {
        MessageListenerContainer contenedor = contenedor();
        boolean encendido = contenedor != null && contenedor.isRunning();

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("encendido", encendido);
        respuesta.put("mensaje", encendido ? "Consumidor encendido" : "Consumidor apagado");
        return respuesta;
    }

    public Map<String, Object> encender() {
        MessageListenerContainer contenedor = requerirContenedor();
        if (!contenedor.isRunning()) {
            contenedor.start();
        }
        return estado();
    }

    public Map<String, Object> apagar() {
        MessageListenerContainer contenedor = requerirContenedor();
        if (contenedor.isRunning()) {
            contenedor.stop();
        }
        return estado();
    }

    public Map<String, Object> consumir(ConsumirRequest request) {
        return colaService.consumir(request);
    }

    private MessageListenerContainer contenedor() {
        return registry.getListenerContainer(RabbitMQConfig.CONSUMIDOR_ID);
    }

    private MessageListenerContainer requerirContenedor() {
        MessageListenerContainer contenedor = contenedor();
        if (contenedor == null) {
            throw new IllegalStateException("El consumidor no esta registrado");
        }
        return contenedor;
    }
}
