package com.minipedidos.controller;

import com.minipedidos.dto.CrearBindingRequest;
import com.minipedidos.dto.CrearColaRequest;
import com.minipedidos.dto.CrearExchangeRequest;
import com.minipedidos.service.RabbitAdminService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * API de administracion de RabbitMQ.
 *
 * Ejemplos de la pauta:
 *   POST   /api/rabbit/queues
 *   DELETE /api/rabbit/queues/{nombre}
 *   POST   /api/rabbit/exchanges
 *   DELETE /api/rabbit/exchanges/{nombre}
 *   POST   /api/rabbit/bindings
 *   DELETE /api/rabbit/bindings
 */
@RestController
@RequestMapping("/api/rabbit")
public class RabbitAdminController {

    private final RabbitAdminService rabbitAdminService;

    public RabbitAdminController(RabbitAdminService rabbitAdminService) {
        this.rabbitAdminService = rabbitAdminService;
    }

    @PostMapping("/queues")
    public ResponseEntity<Map<String, Object>> crearCola(
            @Valid @RequestBody CrearColaRequest request) {

        rabbitAdminService.crearCola(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(respuesta("Cola creada", "cola", request.nombre()));
    }

    @DeleteMapping("/queues/{nombre}")
    public Map<String, Object> eliminarCola(@PathVariable String nombre) {
        boolean eliminada = rabbitAdminService.eliminarCola(nombre);
        Map<String, Object> respuesta = respuesta(
                eliminada ? "Cola eliminada" : "La cola no existia",
                "cola", nombre
        );
        respuesta.put("eliminada", eliminada);
        return respuesta;
    }

    @PostMapping("/exchanges")
    public ResponseEntity<Map<String, Object>> crearExchange(
            @Valid @RequestBody CrearExchangeRequest request) {

        rabbitAdminService.crearExchange(request);
        return ResponseEntity.status(HttpStatus.CREATED)
                .body(respuesta("Exchange creado", "exchange", request.nombre()));
    }

    @DeleteMapping("/exchanges/{nombre}")
    public Map<String, Object> eliminarExchange(@PathVariable String nombre) {
        boolean eliminado = rabbitAdminService.eliminarExchange(nombre);
        Map<String, Object> respuesta = respuesta(
                eliminado ? "Exchange eliminado" : "El exchange no existia",
                "exchange", nombre
        );
        respuesta.put("eliminado", eliminado);
        return respuesta;
    }

    @PostMapping("/bindings")
    public ResponseEntity<Map<String, Object>> crearBinding(
            @Valid @RequestBody CrearBindingRequest request) {

        rabbitAdminService.crearBinding(request);
        Map<String, Object> respuesta = respuesta(
                "Binding creado", "exchange", request.exchange());
        respuesta.put("cola", request.cola());
        respuesta.put("routingKey", request.routingKey());
        return ResponseEntity.status(HttpStatus.CREATED).body(respuesta);
    }

    @DeleteMapping("/bindings")
    public Map<String, Object> eliminarBinding(
            @Valid @RequestBody CrearBindingRequest request) {

        rabbitAdminService.eliminarBinding(request);
        Map<String, Object> respuesta = respuesta(
                "Binding eliminado", "exchange", request.exchange());
        respuesta.put("cola", request.cola());
        respuesta.put("routingKey", request.routingKey());
        return respuesta;
    }

    private Map<String, Object> respuesta(String mensaje, String clave, String valor) {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("mensaje", mensaje);
        respuesta.put(clave, valor);
        return respuesta;
    }
}
