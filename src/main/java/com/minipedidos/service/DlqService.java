package com.minipedidos.service;

import tools.jackson.databind.ObjectMapper;
import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.model.MensajeDlq;
import com.minipedidos.model.Pedido;
import com.rabbitmq.client.Channel;
import org.springframework.amqp.rabbit.connection.Connection;
import com.rabbitmq.client.GetResponse;
import com.rabbitmq.client.LongString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Base64;
import java.util.Date;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Inspeccion, reproceso y descarte de mensajes de la DLQ.
 *
 * AMQP no tiene un "peek" nativo. Se usa basicGet con autoAck=false, se
 * recopilan los mensajes y al final se hace basicNack(requeue=true) para
 * devolverlos a la cola sin perderlos.
 */
@Service
public class DlqService {

    private static final Logger log = LoggerFactory.getLogger(DlqService.class);

    private final ConnectionFactory connectionFactory;
    private final RabbitTemplate rabbitTemplate;
    private final AmqpAdmin amqpAdmin;
    private final TopologiaService topologiaService;
    private final EventoService eventos;
    private final ObjectMapper objectMapper;
    private final int maxInspeccion;
    private final Object lock = new Object();

    public DlqService(
            ConnectionFactory connectionFactory,
            RabbitTemplate rabbitTemplate,
            AmqpAdmin amqpAdmin,
            TopologiaService topologiaService,
            EventoService eventos,
            ObjectMapper objectMapper,
            @Value("${demo.dlq.max-inspeccion:100}") int maxInspeccion) {

        this.connectionFactory = connectionFactory;
        this.rabbitTemplate = rabbitTemplate;
        this.amqpAdmin = amqpAdmin;
        this.topologiaService = topologiaService;
        this.eventos = eventos;
        this.objectMapper = objectMapper;
        this.maxInspeccion = Math.max(1, maxInspeccion);
    }

    public List<MensajeDlq> listar() {
        String cola = topologiaService.getTopologia().colaDlq();
        if (!topologiaService.isConectado()) {
            return List.of();
        }
        synchronized (lock) {
            try {
                if (amqpAdmin.getQueueInfo(cola) == null) {
                    return List.of();
                }
            } catch (Exception e) {
                // RabbitMQ no disponible: la UI muestra el estado desconectado.
                log.debug("No se pudo consultar la DLQ {}: {}", cola, e.getMessage());
                return List.of();
            }

            Connection conexion = null;
            Channel canal = null;
            List<GetResponse> respuestas = new ArrayList<>();

            try {
                conexion = connectionFactory.createConnection();
                canal = conexion.createChannel(false);

                GetResponse respuesta;
                while (respuestas.size() < maxInspeccion
                        && (respuesta = canal.basicGet(cola, false)) != null) {
                    respuestas.add(respuesta);
                }

                List<MensajeDlq> mensajes = respuestas.stream()
                        .map(this::convertir)
                        .toList();

                // Devolver todo a la cola: la inspeccion no consume.
                for (GetResponse item : respuestas) {
                    canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                }

                return mensajes;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "No se pudo inspeccionar la DLQ " + cola + ": " + e.getMessage(), e);
            } finally {
                cerrar(canal);
                cerrar(conexion);
            }
        }
    }

    public Map<String, Object> accion(String id, String accion) {
        String accionNormalizada = accion == null ? "" : accion.trim().toLowerCase();

        if ("inspeccionar".equals(accionNormalizada)) {
            MensajeDlq encontrado = listar().stream()
                    .filter(m -> coincide(m, id))
                    .findFirst()
                    .orElseThrow(() -> new NoSuchElementException(
                            "No se encontro el mensaje " + id + " en la DLQ"));

            eventos.publicar("DLQ_INSPECCIONADA",
                    "Mensaje " + id + " inspeccionado en la DLQ",
                    EventoService.datos(
                            "id", encontrado.id(),
                            "pedidoId", encontrado.pedidoId(),
                            "motivo", encontrado.motivo(),
                            "reintentos", encontrado.reintentos()
                    ));

            return respuesta("inspeccionar", encontrado);
        }

        if (!"reprocesar".equals(accionNormalizada)
                && !"descartar".equals(accionNormalizada)) {
            throw new IllegalArgumentException("Accion no soportada: " + accion);
        }

        String cola = topologiaService.getTopologia().colaDlq();

        synchronized (lock) {
            Connection conexion = null;
            Channel canal = null;
            List<GetResponse> respuestas = new ArrayList<>();

            try {
                conexion = connectionFactory.createConnection();
                canal = conexion.createChannel(false);

                GetResponse respuesta;
                while (respuestas.size() < maxInspeccion
                        && (respuesta = canal.basicGet(cola, false)) != null) {
                    respuestas.add(respuesta);
                }

                GetResponse objetivo = respuestas.stream()
                        .filter(r -> coincide(convertir(r), id))
                        .findFirst()
                        .orElse(null);

                if (objetivo == null) {
                    for (GetResponse item : respuestas) {
                        canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                    }
                    throw new NoSuchElementException(
                            "No se encontro el mensaje " + id + " en la DLQ " + cola);
                }

                MensajeDlq detalle = convertir(objetivo);

                if ("reprocesar".equals(accionNormalizada)) {
                    try {
                        rabbitTemplate.send(
                                RabbitMQConfig.EXCHANGE_PEDIDOS,
                                RabbitMQConfig.ROUTING_KEY,
                                crearMensajeReproceso(objetivo, detalle)
                        );
                    } catch (Exception e) {
                        // No consumir el mensaje si no se pudo republicar.
                        for (GetResponse item : respuestas) {
                            canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                        }
                        throw new IllegalStateException(
                                "No se pudo republicar el mensaje " + id
                                        + ": " + e.getMessage(), e);
                    }
                    log.info("Mensaje {} reprocesado desde la DLQ {}", id, cola);
                } else {
                    log.info("Mensaje {} descartado de la DLQ {}", id, cola);
                }

                for (GetResponse item : respuestas) {
                    long tag = item.getEnvelope().getDeliveryTag();
                    if (item == objetivo) {
                        canal.basicAck(tag, false);
                    } else {
                        canal.basicNack(tag, false, true);
                    }
                }

                eventos.publicar(
                        "reprocesar".equals(accionNormalizada)
                                ? "DLQ_REPROCESADO" : "DLQ_DESCARTADO",
                        "Mensaje " + id + " "
                                + ("reprocesar".equals(accionNormalizada)
                                ? "republicado en " + RabbitMQConfig.EXCHANGE_PEDIDOS
                                : "descartado de la DLQ"),
                        EventoService.datos(
                                "id", detalle.id(),
                                "pedidoId", detalle.pedidoId(),
                                "messageId", detalle.messageId(),
                                "accion", accionNormalizada,
                                "motivo", detalle.motivo()
                        ));

                return respuesta(accionNormalizada, detalle);

            } catch (NoSuchElementException | IllegalArgumentException e) {
                throw e;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "No se pudo ejecutar la accion " + accion
                                + " sobre la DLQ " + cola + ": " + e.getMessage(), e);
            } finally {
                cerrar(canal);
                cerrar(conexion);
            }
        }
    }

    public long purgar() {
        String cola = topologiaService.getTopologia().colaDlq();
        long antes = 0;
        var info = amqpAdmin.getQueueInfo(cola);
        if (info != null) {
            antes = info.getMessageCount();
        }
        amqpAdmin.purgeQueue(cola, false);

        eventos.publicar("DLQ_PURGADA",
                "DLQ " + cola + " vaciada",
                EventoService.datos("cola", cola, "mensajesEliminados", antes));

        return antes;
    }

    private Message crearMensajeReproceso(GetResponse original, MensajeDlq detalle) {
        MessageProperties props = new MessageProperties();
        props.setMessageId(original.getProps().getMessageId() != null
                ? original.getProps().getMessageId()
                : detalle.id());
        props.setContentType(original.getProps().getContentType() != null
                ? original.getProps().getContentType()
                : MessageProperties.CONTENT_TYPE_JSON);
        props.setContentEncoding(original.getProps().getContentEncoding());
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setTimestamp(new Date());
        props.setHeader(RabbitMQConfig.HEADER_ORIGEN, "dlq-reproceso");
        props.setHeader(RabbitMQConfig.HEADER_REINTENTOS, 0);
        props.setHeader("x-reprocesado", true);
        if (detalle.pedidoId() != null) {
            props.setHeader("x-pedido-id", detalle.pedidoId());
        }

        Map<String, Object> headers = original.getProps().getHeaders();
        if (headers != null) {
            headers.forEach((clave, valor) -> {
                if (!"x-death".equals(clave)
                        && !RabbitMQConfig.HEADER_REINTENTOS.equals(clave)
                        && !RabbitMQConfig.HEADER_ULTIMO_ERROR.equals(clave)
                        && !RabbitMQConfig.HEADER_INTENTOS.equals(clave)) {
                    props.setHeader(clave, valor);
                }
            });
        }

        return new Message(original.getBody(), props);
    }

    private MensajeDlq convertir(GetResponse respuesta) {
        String payload = new String(respuesta.getBody(), StandardCharsets.UTF_8);
        Map<String, Object> headers = respuesta.getProps().getHeaders() == null
                ? Map.of()
                : respuesta.getProps().getHeaders();

        Pedido pedido = parsear(payload);
        Long pedidoId = pedido != null ? pedido.getPedidoId() : leerLong(headers.get("x-pedido-id"));
        String messageId = respuesta.getProps().getMessageId();
        String id = messageId != null && !messageId.isBlank()
                ? messageId
                : (pedidoId != null
                ? "pedido-" + pedidoId
                : "dlq-" + Integer.toHexString(payload.hashCode()));

        Map<String, Object> primerXDeath = primerXDeath(headers);
        String motivo = leerString(headers.get(RabbitMQConfig.HEADER_ULTIMO_ERROR));
        if (motivo == null && primerXDeath != null) {
            motivo = leerString(primerXDeath.get("reason"));
        }
        if (motivo == null) {
            motivo = "sin detalle";
        }

        Integer reintentos = leerInteger(headers.get(RabbitMQConfig.HEADER_REINTENTOS));
        if (reintentos == 0) {
            Integer intentos = leerInteger(headers.get(RabbitMQConfig.HEADER_INTENTOS));
            reintentos = intentos != null ? intentos : 0;
        }

        Date fecha = respuesta.getProps().getTimestamp();

        return new MensajeDlq(
                id,
                messageId,
                pedidoId,
                pedido != null ? pedido.getCliente() : null,
                pedido != null ? pedido.getProducto() : null,
                pedido != null ? pedido.getCantidad() : null,
                motivo,
                primerXDeath != null ? leerString(primerXDeath.get("queue")) : null,
                primerXDeath != null ? leerString(primerXDeath.get("exchange")) : null,
                routingKey(primerXDeath),
                reintentos,
                payload,
                simplificarMapa(headers),
                fecha == null ? null : fecha.getTime()
        );
    }

    private boolean coincide(MensajeDlq mensaje, String id) {
        if (id == null) {
            return false;
        }
        String buscado = id.trim();
        return buscado.equals(mensaje.id())
                || buscado.equals(mensaje.messageId())
                || (mensaje.pedidoId() != null
                && buscado.equals(String.valueOf(mensaje.pedidoId())));
    }

    private Map<String, Object> respuesta(String accion, MensajeDlq mensaje) {
        Map<String, Object> respuesta = new LinkedHashMap<>();
        respuesta.put("mensaje", switch (accion) {
            case "inspeccionar" -> "Mensaje inspeccionado; sigue en la DLQ";
            case "reprocesar" -> "Mensaje republicado en la cola principal";
            case "descartar" -> "Mensaje eliminado de la DLQ";
            default -> "Accion ejecutada";
        });
        respuesta.put("accion", accion);
        respuesta.put("detalle", mensaje);
        return respuesta;
    }

    private Pedido parsear(String payload) {
        try {
            return objectMapper.readValue(payload, Pedido.class);
        } catch (Exception e) {
            return null;
        }
    }

    @SuppressWarnings("unchecked")
    private Map<String, Object> primerXDeath(Map<String, Object> headers) {
        Object xDeath = headers.get("x-death");
        if (xDeath instanceof List<?> lista && !lista.isEmpty()) {
            Object primero = lista.get(0);
            if (primero instanceof Map<?, ?> mapa) {
                return (Map<String, Object>) mapa;
            }
        }
        return null;
    }

    private String routingKey(Map<String, Object> xDeath) {
        if (xDeath == null) {
            return null;
        }
        Object routingKeys = xDeath.get("routing-keys");
        if (routingKeys instanceof List<?> lista && !lista.isEmpty()) {
            return leerString(lista.get(0));
        }
        return leerString(xDeath.get("routing-key"));
    }

    private Map<String, Object> simplificarMapa(Map<String, Object> mapa) {
        Map<String, Object> resultado = new LinkedHashMap<>();
        if (mapa == null) {
            return resultado;
        }
        mapa.forEach((clave, valor) -> resultado.put(clave, simplificar(valor)));
        return resultado;
    }

    private Object simplificar(Object valor) {
        if (valor instanceof LongString texto) {
            return texto.toString();
        }
        if (valor instanceof byte[] bytes) {
            return Base64.getEncoder().encodeToString(bytes);
        }
        if (valor instanceof List<?> lista) {
            return lista.stream().map(this::simplificar).toList();
        }
        if (valor instanceof Map<?, ?> mapa) {
            Map<String, Object> resultado = new LinkedHashMap<>();
            mapa.forEach((clave, item) ->
                    resultado.put(String.valueOf(clave), simplificar(item)));
            return resultado;
        }
        return valor;
    }

    private String leerString(Object valor) {
        return valor == null ? null : valor.toString();
    }

    private Integer leerInteger(Object valor) {
        if (valor instanceof Number numero) {
            return numero.intValue();
        }
        if (valor instanceof String texto) {
            try {
                return Integer.parseInt(texto);
            } catch (NumberFormatException ignored) {
                return 0;
            }
        }
        return 0;
    }

    private Long leerLong(Object valor) {
        if (valor instanceof Number numero) {
            return numero.longValue();
        }
        if (valor instanceof String texto) {
            try {
                return Long.parseLong(texto);
            } catch (NumberFormatException ignored) {
                return null;
            }
        }
        return null;
    }

    private void cerrar(AutoCloseable recurso) {
        if (recurso == null) {
            return;
        }
        try {
            recurso.close();
        } catch (Exception e) {
            log.debug("Error cerrando recurso AMQP: {}", e.getMessage());
        }
    }
}
