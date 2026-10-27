package com.minipedidos.controller;

import com.minipedidos.dto.AccionDlqRequest;
import com.minipedidos.model.MensajeDlq;
import com.minipedidos.service.DlqService;
import com.minipedidos.service.TopologiaService;
import jakarta.validation.Valid;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/dlq")
public class DlqController {

    private final DlqService dlqService;
    private final TopologiaService topologiaService;

    public DlqController(DlqService dlqService, TopologiaService topologiaService) {
        this.dlqService = dlqService;
        this.topologiaService = topologiaService;
    }

    @GetMapping
    public Map<String, Object> listar() {
        List<MensajeDlq> mensajes = dlqService.listar();
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("cola", topologiaService.getTopologia().colaDlq());
        respuesta.put("total", mensajes.size());
        respuesta.put("mensajes", mensajes);
        return respuesta;
    }

    @PostMapping("/accion")
    public Map<String, Object> accion(@Valid @RequestBody AccionDlqRequest request) {
        return dlqService.accion(request.mensajeDlq(), request.accionDlq());
    }

    @PostMapping("/purgar")
    public Map<String, Object> purgar() {
        long eliminados = dlqService.purgar();
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("mensaje", "DLQ vaciada");
        respuesta.put("eliminados", eliminados);
        return respuesta;
    }
}
