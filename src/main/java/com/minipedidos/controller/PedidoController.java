package com.minipedidos.controller;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.dto.SimularErrorRequest;
import com.minipedidos.model.Pedido;
import com.minipedidos.service.PedidoService;
import com.minipedidos.service.TopologiaService;
import jakarta.validation.Valid;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

@RestController
@RequestMapping("/api/pedidos")
public class PedidoController {

    private final PedidoService pedidoService;
    private final TopologiaService topologiaService;

    public PedidoController(
            PedidoService pedidoService,
            TopologiaService topologiaService) {

        this.pedidoService = pedidoService;
        this.topologiaService = topologiaService;
    }

    @PostMapping
    public ResponseEntity<Map<String, Object>> crearPedido(
            @Valid @RequestBody Pedido pedido) {

        Pedido creado = pedidoService.crear(pedido);
        return ResponseEntity.accepted().body(respuesta(creado,
                "Pedido enviado a procesamiento"));
    }

    @PostMapping("/simular")
    public ResponseEntity<Map<String, Object>> simularError(
            @Valid @RequestBody SimularErrorRequest request) {

        Pedido creado = pedidoService.simular(
                request.cliente(),
                request.tipoError().toLowerCase()
        );
        return ResponseEntity.accepted().body(respuesta(creado,
                "Pedido de simulacion enviado. Revisa el monitor y la DLQ."));
    }

    @GetMapping
    public List<Pedido> listar() {
        return pedidoService.listar();
    }

    @GetMapping("/{id}")
    public Pedido obtener(@PathVariable Long id) {
        Pedido pedido = pedidoService.obtener(id);
        if (pedido == null) {
            throw new NoSuchElementException("No existe el pedido " + id);
        }
        return pedido;
    }

    private Map<String, Object> respuesta(Pedido pedido, String mensaje) {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("mensaje", mensaje);
        respuesta.put("pedidoId", pedido.getPedidoId());
        respuesta.put("messageId", pedido.getMessageId());
        respuesta.put("estado", pedido.getEstado());
        respuesta.put("cliente", pedido.getCliente());
        respuesta.put("producto", pedido.getProducto());
        respuesta.put("cantidad", pedido.getCantidad());
        respuesta.put("exchange", RabbitMQConfig.EXCHANGE_PEDIDOS);
        respuesta.put("routingKey", RabbitMQConfig.ROUTING_KEY);
        respuesta.put("cola", topologiaService.getTopologia().colaPrincipal());
        respuesta.put("tipoError", pedido.getTipoError());
        return respuesta;
    }
}
