package com.minipedidos.controller;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.model.MensajeCola;
import com.minipedidos.service.ColaService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/colas")
public class ColaController {

    private final ColaService colaService;

    public ColaController(ColaService colaService) {
        this.colaService = colaService;
    }

    @GetMapping("/mensajes")
    public Map<String, Object> mensajes(
            @RequestParam(defaultValue = RabbitMQConfig.COLA_PEDIDOS_DEFAULT) String cola) {

        List<MensajeCola> mensajes = colaService.listar(cola);

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("cola", cola);
        respuesta.put("total", mensajes.size());
        respuesta.put("mensajes", mensajes);
        return respuesta;
    }
}
