package com.minipedidos.service;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.consumer.PedidoConsumer;
import com.minipedidos.dto.ConsumirRequest;
import com.minipedidos.model.MensajeCola;
import com.minipedidos.model.Pedido;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.GetResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.connection.Connection;
import org.springframework.amqp.rabbit.connection.ConnectionFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import tools.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

/**
 * Lista los mensajes que estan esperando en una cola y permite consumir uno
 * puntual de forma manual.
 *
 * Para listar se usa basicGet con autoAck=false y luego basicNack(requeue=true):
 * el mensaje se inspecciona pero no se pierde.
 */
@Service
public class ColaService {

    private static final Logger log = LoggerFactory.getLogger(ColaService.class);

    private final ConnectionFactory connectionFactory;
    private final PedidoConsumer pedidoConsumer;
    private final TopologiaService topologiaService;
    private final ObjectMapper objectMapper;
    private final int maxInspeccion;
    private final Object lock = new Object();

    public ColaService(
            ConnectionFactory connectionFactory,
            PedidoConsumer pedidoConsumer,
            TopologiaService topologiaService,
            ObjectMapper objectMapper,
            @Value("${demo.cola.max-inspeccion:50}") int maxInspeccion) {

        this.connectionFactory = connectionFactory;
        this.pedidoConsumer = pedidoConsumer;
        this.topologiaService = topologiaService;
        this.objectMapper = objectMapper;
        this.maxInspeccion = Math.max(1, maxInspeccion);
    }

    public List<MensajeCola> listar(String cola) {
        validarCola(cola);

        if (!topologiaService.isConectado()) {
            return List.of();
        }

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

                List<MensajeCola> mensajes = respuestas.stream()
                        .map((r) -> convertir(r, cola))
                        .toList();

                for (GetResponse item : respuestas) {
                    canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                }

                return mensajes;
            } catch (Exception e) {
                throw new IllegalStateException(
                        "No se pudo leer la cola " + cola + ": " + e.getMessage(), e);
            } finally {
                cerrar(canal);
                cerrar(conexion);
            }
        }
    }

    public Map<String, Object> consumir(ConsumirRequest request) {
        validarCola(request.cola());

        if (!topologiaService.isConectado()) {
            throw new IllegalStateException("RabbitMQ no esta conectado");
        }

        String cola = request.cola();
        String tipoError = request.tipoError();
        boolean normal = tipoError == null || tipoError.isBlank();

        synchronized (lock) {
            Connection conexion = null;
            Channel canal = null;
            List<GetResponse> respuestas = new ArrayList<>();

            try {
                conexion = connectionFactory.createConnection();
                canal = conexion.createChannel(false);

                GetResponse objetivo = null;
                GetResponse respuesta;
                while (respuestas.size() < maxInspeccion
                        && (respuesta = canal.basicGet(cola, false)) != null) {
                    respuestas.add(respuesta);
                    if (coincide(respuesta, request.mensajeId())) {
                        objetivo = respuesta;
                        break;
                    }
                }

                if (objetivo == null) {
                    for (GetResponse item : respuestas) {
                        canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                    }
                    throw new NoSuchElementException(
                            "No se encontro el mensaje " + request.mensajeId()
                                    + " en la cola " + cola
                                    + ". Si el consumidor esta encendido, apagalo primero.");
                }

                Pedido pedido = parsear(objetivo.getBody());
                if (pedido == null) {
                    for (GetResponse item : respuestas) {
                        canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                    }
                    throw new IllegalArgumentException(
                            "El mensaje seleccionado no es un Pedido valido");
                }

                // Normal limpia cualquier error de una simulacion anterior.
                pedido.setTipoError(normal ? null : tipoError);
                byte[] cuerpo = objectMapper.writeValueAsBytes(pedido);

                MessageProperties props = new MessageProperties();
                props.setDeliveryTag(objetivo.getEnvelope().getDeliveryTag());
                props.setConsumerQueue(cola);
                props.setMessageId(objetivo.getProps().getMessageId());
                props.setContentType(MessageProperties.CONTENT_TYPE_JSON);
                props.setContentEncoding(StandardCharsets.UTF_8.name());
                props.setHeader("x-consumo-manual", true);

                Map<String, Object> headers = objetivo.getProps().getHeaders();
                if (headers != null) {
                    props.setHeaders(new LinkedHashMap<>(headers));
                }

                Message mensaje = new Message(cuerpo, props);
                pedidoConsumer.procesar(mensaje, canal);

                // El objetivo ya fue ACK/NACK por el consumidor. Los demas se devuelven.
                for (GetResponse item : respuestas) {
                    if (item != objetivo) {
                        canal.basicNack(item.getEnvelope().getDeliveryTag(), false, true);
                    }
                }

                log.info("Consumo manual: cola={}, mensajeId={}, accion={}",
                        cola, request.mensajeId(), normal ? "normal" : tipoError);

                Map<String, Object> resultado = new LinkedHashMap<>();
                resultado.put("mensaje", "Mensaje consumido: " + request.mensajeId());
                resultado.put("accion", normal ? "normal" : tipoError);
                resultado.put("mensajeId", request.mensajeId());
                return resultado;

            } catch (NoSuchElementException | IllegalArgumentException e) {
                throw e;
            } catch (IOException e) {
                throw new IllegalStateException(
                        "No se pudo consumir el mensaje: " + e.getMessage(), e);
            } catch (Exception e) {
                throw new IllegalStateException(
                        "No se pudo consumir el mensaje: " + e.getMessage(), e);
            } finally {
                cerrar(canal);
                cerrar(conexion);
            }
        }
    }

    private MensajeCola convertir(GetResponse respuesta, String cola) {
        String payload = new String(respuesta.getBody(), StandardCharsets.UTF_8);
        Pedido pedido = parsear(respuesta.getBody());
        Map<String, Object> headers = respuesta.getProps().getHeaders() == null
                ? Map.of()
                : respuesta.getProps().getHeaders();

        Long pedidoId = pedido != null ? pedido.getPedidoId() : null;
        String messageId = respuesta.getProps().getMessageId();
        String id = messageId != null && !messageId.isBlank()
                ? messageId
                : (pedidoId != null
                ? "pedido-" + pedidoId
                : "msg-" + Integer.toHexString(payload.hashCode()));

        int reintentos = leerEntero(headers.get(RabbitMQConfig.HEADER_REINTENTOS));
        if (reintentos == 0) {
            reintentos = leerEntero(headers.get(RabbitMQConfig.HEADER_INTENTOS));
        }

        return new MensajeCola(
                id,
                messageId,
                pedidoId,
                pedido != null ? pedido.getCliente() : null,
                pedido != null ? pedido.getProducto() : null,
                pedido != null ? pedido.getCantidad() : null,
                pedido != null ? pedido.getEstado() : null,
                reintentos,
                payload
        );
    }

    private boolean coincide(GetResponse respuesta, String id) {
        if (id == null) {
            return false;
        }

        String buscado = id.trim();
        String messageId = respuesta.getProps().getMessageId();
        if (buscado.equals(messageId)) {
            return true;
        }

        Map<String, Object> headers = respuesta.getProps().getHeaders();
        if (headers != null) {
            Object xPedido = headers.get("x-pedido-id");
            if (xPedido != null && buscado.equals(xPedido.toString())) {
                return true;
            }
        }

        Pedido pedido = parsear(respuesta.getBody());
        return pedido != null
                && pedido.getPedidoId() != null
                && buscado.equals(String.valueOf(pedido.getPedidoId()));
    }

    private Pedido parsear(byte[] body) {
        try {
            return objectMapper.readValue(body, Pedido.class);
        } catch (Exception e) {
            return null;
        }
    }

    private int leerEntero(Object valor) {
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

    private void validarCola(String cola) {
        if (cola == null || cola.isBlank()) {
            throw new IllegalArgumentException("La cola es obligatoria");
        }
        String principal = topologiaService.getTopologia().colaPrincipal();
        if (!cola.equals(principal) && !cola.equals(RabbitMQConfig.COLA_REINTENTOS)) {
            throw new IllegalArgumentException("Cola no permitida: " + cola);
        }
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
