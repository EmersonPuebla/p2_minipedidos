package com.minipedidos.controller;

import com.minipedidos.dto.LoteRequest;
import com.minipedidos.model.Pedido;
import com.minipedidos.service.PedidoService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("/api/demo")
public class DemoController {

    private final PedidoService pedidoService;

    public DemoController(PedidoService pedidoService) {
        this.pedidoService = pedidoService;
    }

    @PostMapping("/lote")
    public ResponseEntity<Map<String, Object>> lote(
            @Valid @RequestBody LoteRequest request) {

        List<Pedido> pedidos = pedidoService.crearLote(request.cantidad());

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("mensaje", "Se enviaron " + pedidos.size()
                + " pedidos. Observa el reparto entre consumidores.");
        respuesta.put("cantidad", pedidos.size());
        respuesta.put("pedidos", pedidos);
        return ResponseEntity.accepted().body(respuesta);
    }
}
