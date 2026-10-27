package com.minipedidos.service;

import tools.jackson.databind.ObjectMapper;
import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.model.PoliticaReintentos;
import com.minipedidos.model.Topologia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.QueueInformation;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Base64;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Metricas para el panel de la demo.
 *
 * Intenta usar la API HTTP de RabbitMQ Management (puerto 15672) para obtener
 * ready/unacked/consumers reales. Si no esta disponible, cae a
 * {@link AmqpAdmin#getQueueInfo(String)}.
 */
@Service
public class MetricasService {

    private static final Logger log = LoggerFactory.getLogger(MetricasService.class);

    private final TopologiaService topologiaService;
    private final PoliticaReintentosService politicaService;
    private final PedidoStore pedidoStore;
    private final AmqpAdmin amqpAdmin;
    private final ObjectMapper objectMapper;
    private final String managementUrl;
    private final String managementUser;
    private final String managementPassword;
    private final int consumidores;
    private final HttpClient httpClient;

    public MetricasService(
            TopologiaService topologiaService,
            PoliticaReintentosService politicaService,
            PedidoStore pedidoStore,
            AmqpAdmin amqpAdmin,
            ObjectMapper objectMapper,
            @Value("${rabbitmq.management.url:http://127.0.0.1:15672}") String managementUrl,
            @Value("${rabbitmq.management.username:guest}") String managementUser,
            @Value("${rabbitmq.management.password:guest}") String managementPassword,
            @Value("${demo.consumidores:2}") int consumidores) {

        this.topologiaService = topologiaService;
        this.politicaService = politicaService;
        this.pedidoStore = pedidoStore;
        this.amqpAdmin = amqpAdmin;
        this.objectMapper = objectMapper;
        this.managementUrl = managementUrl.endsWith("/")
                ? managementUrl.substring(0, managementUrl.length() - 1)
                : managementUrl;
        this.managementUser = managementUser;
        this.managementPassword = managementPassword;
        this.consumidores = consumidores;
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(Duration.ofMillis(900))
                .build();
    }

    public Map<String, Object> obtener() {
        Topologia topologia = topologiaService.getTopologia();
        PoliticaReintentos politica = politicaService.getPolitica();
        boolean[] gestion = {false};

        List<Map<String, Object>> colas = new ArrayList<>();
        colas.add(estadistica(topologia.colaPrincipal(), "principal", gestion));
        colas.add(estadistica(topologia.colaDlq(), "dlq", gestion));
        colas.add(estadistica(RabbitMQConfig.COLA_REINTENTOS, "reintentos", gestion));

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("rabbitmqConectado",
                topologiaService.isConectado() || gestion[0]);
        respuesta.put("gestionDisponible", gestion[0]);
        respuesta.put("topologia", topologia);
        respuesta.put("politica", politica);
        respuesta.put("concurrencia", consumidores);
        respuesta.put("resumenPedidos", pedidoStore.resumen());
        respuesta.put("colas", colas);
        respuesta.put("exchangePrincipal", RabbitMQConfig.EXCHANGE_PEDIDOS);
        respuesta.put("routingKey", RabbitMQConfig.ROUTING_KEY);
        respuesta.put("exchangeReintentos", RabbitMQConfig.EXCHANGE_REINTENTOS);
        respuesta.put("colaReintentos", RabbitMQConfig.COLA_REINTENTOS);
        respuesta.put("routingKeyReintentos", RabbitMQConfig.ROUTING_KEY_REINTENTOS);
        return respuesta;
    }

    public Map<String, Object> salud() {
        boolean rabbitmq = false;
        try {
            String cola = topologiaService.getTopologia().colaPrincipal();
            rabbitmq = consultarManagement(cola) != null
                    || (topologiaService.isConectado()
                    && amqpAdmin.getQueueInfo(cola) != null);
        } catch (Exception e) {
            log.debug("Salud RabbitMQ no disponible: {}", e.getMessage());
        }

        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("aplicacion", "UP");
        respuesta.put("rabbitmq", rabbitmq);
        respuesta.put("concurrencia", consumidores);
        return respuesta;
    }

    private Map<String, Object> estadistica(
            String nombre,
            String rol,
            boolean[] gestion) {

        Map<String, Object> stats = new LinkedHashMap<>();
        stats.put("nombre", nombre);
        stats.put("rol", rol);

        Map<String, Object> data = consultarManagement(nombre);
        if (data != null) {
            gestion[0] = true;
            stats.put("ready", numero(data.get("messages_ready")));
            stats.put("unacked", numero(data.get("messages_unacknowledged")));
            stats.put("total", numero(data.get("messages")));
            stats.put("consumidores", numero(data.get("consumers")));
            stats.put("estado", data.getOrDefault("state", "running"));
            stats.put("fuente", "management");
            return stats;
        }

        // Si RabbitMQ nunca se conecto, no intentamos abrir una conexion AMQP
        // en cada refresco del panel (evita timeouts y logs repetidos).
        if (!topologiaService.isConectado()) {
            stats.put("ready", 0);
            stats.put("unacked", 0);
            stats.put("total", 0);
            stats.put("consumidores", 0);
            stats.put("estado", "desconectado");
            stats.put("fuente", "ninguna");
            return stats;
        }

        try {
            QueueInformation info = amqpAdmin.getQueueInfo(nombre);
            if (info != null) {
                stats.put("ready", info.getMessageCount());
                stats.put("unacked", 0);
                stats.put("total", info.getMessageCount());
                stats.put("consumidores", info.getConsumerCount());
                stats.put("estado", "running");
                stats.put("fuente", "amqp");
                return stats;
            }
        } catch (Exception e) {
            log.debug("No se pudo consultar la cola {}: {}", nombre, e.getMessage());
        }

        stats.put("ready", 0);
        stats.put("unacked", 0);
        stats.put("total", 0);
        stats.put("consumidores", 0);
        stats.put("estado", "no disponible");
        stats.put("fuente", "ninguna");
        return stats;
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> consultarManagement(String cola) {
        try {
            String vhost = URLEncoder.encode("/", StandardCharsets.UTF_8);
            String nombre = URLEncoder.encode(cola, StandardCharsets.UTF_8);
            String url = managementUrl + "/api/queues/" + vhost + "/" + nombre;

            String basic = Base64.getEncoder().encodeToString(
                    (managementUser + ":" + managementPassword)
                            .getBytes(StandardCharsets.UTF_8)
            );

            HttpRequest request = HttpRequest.newBuilder(URI.create(url))
                    .timeout(Duration.ofMillis(900))
                    .header("Authorization", "Basic " + basic)
                    .header("Accept", "application/json")
                    .GET()
                    .build();

            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString()
            );

            if (response.statusCode() == 200) {
                return objectMapper.readValue(response.body(), Map.class);
            }
        } catch (Exception e) {
            log.debug("Management API no disponible: {}", e.getMessage());
        }
        return null;
    }

    private long numero(Object valor) {
        if (valor instanceof Number numero) {
            return numero.longValue();
        }
        if (valor instanceof String texto) {
            try {
                return Long.parseLong(texto);
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }
}
