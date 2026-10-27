package com.minipedidos.controller;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.dto.PoliticaReintentosRequest;
import com.minipedidos.model.PoliticaReintentos;
import com.minipedidos.service.EventoService;
import com.minipedidos.service.PoliticaReintentosService;
import com.minipedidos.service.TopologiaService;
import jakarta.validation.Valid;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.LinkedHashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/configuracion")
public class ConfiguracionController {

    private final TopologiaService topologiaService;
    private final PoliticaReintentosService politicaService;
    private final EventoService eventos;
    private final int consumidores;

    public ConfiguracionController(
            TopologiaService topologiaService,
            PoliticaReintentosService politicaService,
            EventoService eventos,
            @Value("${demo.consumidores:2}") int consumidores) {

        this.topologiaService = topologiaService;
        this.politicaService = politicaService;
        this.eventos = eventos;
        this.consumidores = consumidores;
    }

    @GetMapping
    public Map<String, Object> obtener() {
        return configuracion();
    }

    @PostMapping("/reintentos")
    public Map<String, Object> configurarReintentos(
            @Valid @RequestBody PoliticaReintentosRequest request) {

        PoliticaReintentos nueva = politicaService.actualizar(
                request.maxReintentos(),
                request.intervalo()
        );

        eventos.publicar("POLITICA_ACTUALIZADA",
                "Politica de reintentos: " + nueva.maxReintentos()
                        + " reintento(s) cada " + nueva.intervaloSegundos() + " s",
                EventoService.datos(
                        "maxReintentos", nueva.maxReintentos(),
                        "intervaloSegundos", nueva.intervaloSegundos()
                ));

        Map<String, Object> respuesta = configuracion();
        respuesta.put("mensaje", "Politica de reintentos actualizada");
        respuesta.put("politica", nueva);
        return respuesta;
    }

    @PostMapping("/reconectar")
    public Map<String, Object> reconectar() {
        boolean ok = topologiaService.inicializar();
        Map<String, Object> respuesta = configuracion();
        respuesta.put("mensaje", ok
                ? "Topologia declarada y consumidores activos"
                : "No se pudo inicializar RabbitMQ. Revisa que este iniciado.");
        respuesta.put("conectado", ok);
        return respuesta;
    }

    private Map<String, Object> configuracion() {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("topologia", topologiaService.getTopologia());
        respuesta.put("politica", politicaService.getPolitica());
        respuesta.put("concurrencia", consumidores);
        respuesta.put("conectado", topologiaService.isConectado());
        respuesta.put("exchangePrincipal", RabbitMQConfig.EXCHANGE_PEDIDOS);
        respuesta.put("routingKey", RabbitMQConfig.ROUTING_KEY);
        respuesta.put("exchangeReintentos", RabbitMQConfig.EXCHANGE_REINTENTOS);
        respuesta.put("colaReintentos", RabbitMQConfig.COLA_REINTENTOS);
        respuesta.put("routingKeyReintentos", RabbitMQConfig.ROUTING_KEY_REINTENTOS);
        return respuesta;
    }
}
