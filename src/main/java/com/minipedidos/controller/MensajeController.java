package com.minipedidos.controller;

import com.minipedidos.dto.PublicarMensajeRequest;
import com.minipedidos.service.MensajeService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api/mensajes")
public class MensajeController {

    private final MensajeService mensajeService;

    public MensajeController(MensajeService mensajeService) {
        this.mensajeService = mensajeService;
    }

    @PostMapping("/publicar")
    public ResponseEntity<Map<String, Object>> publicar(
            @Valid @RequestBody PublicarMensajeRequest request) {

        Map<String, Object> respuesta = mensajeService.publicar(
                request.exchange(),
                request.routingKey(),
                request.mensaje()
        );

        return ResponseEntity.accepted().body(respuesta);
    }
}
