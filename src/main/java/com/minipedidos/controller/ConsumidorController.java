package com.minipedidos.controller;

import com.minipedidos.dto.ConsumirRequest;
import com.minipedidos.service.ConsumidorService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/consumidor")
public class ConsumidorController {

    private final ConsumidorService consumidorService;

    public ConsumidorController(ConsumidorService consumidorService) {
        this.consumidorService = consumidorService;
    }

    @GetMapping("/estado")
    public Map<String, Object> estado() {
        return consumidorService.estado();
    }

    @PostMapping("/encender")
    public Map<String, Object> encender() {
        return consumidorService.encender();
    }

    @PostMapping("/apagar")
    public Map<String, Object> apagar() {
        return consumidorService.apagar();
    }

    @PostMapping("/consumir")
    public Map<String, Object> consumir(
            @Valid @RequestBody ConsumirRequest request) {

        return consumidorService.consumir(request);
    }
}
