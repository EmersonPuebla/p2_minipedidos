package com.minipedidos.consumer;

import tools.jackson.databind.ObjectMapper;
import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.exception.ErrorPermanenteException;
import com.minipedidos.exception.ErrorTemporalException;
import com.minipedidos.model.Pedido;
import com.minipedidos.model.PoliticaReintentos;
import com.minipedidos.service.EventoService;
import com.minipedidos.service.PedidoStore;
import com.minipedidos.service.PedidoValidator;
import com.minipedidos.service.PoliticaReintentosService;
import com.minipedidos.service.ReintentoService;
import com.minipedidos.service.SimuladorErrores;
import com.rabbitmq.client.Channel;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.rabbit.annotation.RabbitListener;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Consumidor con ACK manual.
 *
 * Reglas:
 * <ul>
 *   <li>Error permanente: NACK sin requeue -> DLX -> DLQ.</li>
 *   <li>Error temporal: se publica en la cola de reintentos con TTL y se hace
 *       ACK del mensaje original. Si se agotan los reintentos, NACK -> DLQ.</li>
 *   <li>Mensaje duplicado: ACK sin volver a ejecutar el efecto de negocio.</li>
 * </ul>
 */
@Component
public class PedidoConsumer {

    private static final Logger log = LoggerFactory.getLogger(PedidoConsumer.class);

    private final ObjectMapper objectMapper;
    private final PedidoValidator validator;
    private final SimuladorErrores simulador;
    private final PoliticaReintentosService politicaService;
    private final ReintentoService reintentoService;
    private final EventoService eventos;
    private final PedidoStore pedidos;
    private final long delayMs;

    /**
     * Idempotencia en memoria: si el mismo messageId se entrega dos veces, el
     * segundo procesamiento no vuelve a ejecutar el efecto de negocio.
     */
    private final Set<String> mensajesProcesados = ConcurrentHashMap.newKeySet();

    public PedidoConsumer(
            ObjectMapper objectMapper,
            PedidoValidator validator,
            SimuladorErrores simulador,
            PoliticaReintentosService politicaService,
            ReintentoService reintentoService,
            EventoService eventos,
            PedidoStore pedidos,
            @Value("${demo.procesamiento.delay-ms:800}") long delayMs) {

        this.objectMapper = objectMapper;
        this.validator = validator;
        this.simulador = simulador;
        this.politicaService = politicaService;
        this.reintentoService = reintentoService;
        this.eventos = eventos;
        this.pedidos = pedidos;
        this.delayMs = Math.max(0, delayMs);
    }

    /**
     * Consumidor principal del laboratorio.
     *
     * La anotacion crea el contenedor de Spring AMQP y usa 2 consumidores
     * concurrentes por defecto. El ACK/NACK se controla manualmente dentro de
     * {@link #procesar(Message, Channel)}.
     */
    @RabbitListener(
            id = RabbitMQConfig.CONSUMIDOR_ID,
            queues = RabbitMQConfig.COLA_PEDIDOS_DEFAULT,
            concurrency = "${demo.consumidores:2}"
    )
    public void recibirPedido(Message message, Channel channel) throws IOException {
        procesar(message, channel);
    }

    public void procesar(Message message, Channel channel) throws IOException {
        if (message == null || channel == null) {
            return;
        }

        long deliveryTag = message.getMessageProperties().getDeliveryTag();
        String cola = message.getMessageProperties().getConsumerQueue();
        String consumidor = nombreConsumidor();
        String messageId = asegurarMessageId(message);
        int reintento = leerReintento(message);

        Pedido pedido = null;

        try {
            pedido = leerPedido(message);
            if (pedido.getMessageId() == null) {
                pedido.setMessageId(messageId);
            }
            validarPedido(pedido);

            eventos.publicar("PEDIDO_RECIBIDO",
                    "Pedido #" + pedido.getPedidoId() + " recibido por " + consumidor,
                    datos(pedido, messageId, cola, consumidor, reintento, null));

            if (mensajesProcesados.contains(messageId)) {
                channel.basicAck(deliveryTag, false);
                marcar(pedido, "DUPLICADO", reintento, "Mensaje duplicado ignorado");
                eventos.publicar("DUPLICADO_IGNORADO",
                        "Pedido #" + pedido.getPedidoId()
                                + " ya fue procesado; ACK sin repetir el efecto",
                        datos(pedido, messageId, cola, consumidor, reintento,
                                "Idempotencia por messageId"));
                return;
            }

            marcar(pedido, "PROCESANDO", reintento, null);

            // Simulacion de trabajo real. Con 2 consumidores, esta pausa hace
            // visible el reparto de carga en el monitor y en RabbitMQ.
            if (simulador.esTimeout(pedido)) {
                dormir(delayMs * 3);
            }

            simulador.evaluar(pedido, reintento, esReprocesado(message));
            dormir(delayMs);

            // --- Exito: efecto de negocio + ACK ---
            mensajesProcesados.add(messageId);
            marcar(pedido, "PROCESADO", reintento, null);
            channel.basicAck(deliveryTag, false);

            log.info("ACK pedido={}, messageId={}, consumidor={}, intento={}",
                    pedido.getPedidoId(), messageId, consumidor, reintento + 1);

            eventos.publicar("ACK",
                    "Pedido #" + pedido.getPedidoId() + " procesado y confirmado (ACK)",
                    datos(pedido, messageId, cola, consumidor, reintento, null));

        } catch (ErrorPermanenteException e) {
            manejarPermanente(message, channel, deliveryTag, pedido, messageId,
                    cola, consumidor, reintento, e);
        } catch (ErrorTemporalException e) {
            manejarTemporal(message, channel, deliveryTag, pedido, messageId,
                    cola, consumidor, reintento, e);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            manejarPermanente(message, channel, deliveryTag, pedido, messageId,
                    cola, consumidor, reintento,
                    new ErrorPermanenteException("Procesamiento interrumpido", e));
        } catch (Exception e) {
            // JSON invalido, error de conversion o error inesperado: se trata
            // como permanente para no crear un bucle infinito de mensajes veneno.
            manejarPermanente(message, channel, deliveryTag, pedido, messageId,
                    cola, consumidor, reintento,
                    new ErrorPermanenteException(
                            "Mensaje no procesable: " + e.getMessage(), e));
        }
    }

    private void manejarPermanente(
            Message message,
            Channel channel,
            long deliveryTag,
            Pedido pedido,
            String messageId,
            String cola,
            String consumidor,
            int reintento,
            ErrorPermanenteException error) throws IOException {

        log.warn("NACK sin requeue (permanente): messageId={}, motivo={}",
                messageId, error.getMessage());

        marcar(pedido, "FALLIDO", reintento + 1, error.getMessage());
        anotarError(message, error.getMessage(), reintento + 1);

        channel.basicNack(deliveryTag, false, false);

        eventos.publicar("NACK_DLQ",
                "Pedido " + (pedido == null ? messageId : "#" + pedido.getPedidoId())
                        + " rechazado sin requeue -> DLQ",
                datos(pedido, messageId, cola, consumidor, reintento,
                        error.getMessage()));
    }

    private void manejarTemporal(
            Message message,
            Channel channel,
            long deliveryTag,
            Pedido pedido,
            String messageId,
            String cola,
            String consumidor,
            int reintento,
            ErrorTemporalException error) throws IOException {

        PoliticaReintentos politica = politicaService.getPolitica();
        int max = politica.maxReintentos();

        if (pedido == null) {
            // Sin pedido no podemos reconstruir el mensaje para reintentarlo.
            manejarPermanente(message, channel, deliveryTag, null, messageId,
                    cola, consumidor, reintento,
                    new ErrorPermanenteException(error.getMessage(), error));
            return;
        }

        if (reintento < max) {
            int siguiente = reintento + 1;
            try {
                reintentoService.programar(pedido, message, siguiente);
                // El original se confirma; la copia vive en la cola de reintentos.
                channel.basicAck(deliveryTag, false);

                marcar(pedido, "EN_REINTENTO", siguiente, error.getMessage());

                log.info("ACK + reintento programado: pedido={}, intento={}/{}",
                        pedido.getPedidoId(), siguiente, max);

                eventos.publicar("ERROR_TEMPORAL",
                        "Pedido #" + pedido.getPedidoId()
                                + " fallo temporalmente; reintento "
                                + siguiente + "/" + max,
                        datos(pedido, messageId, cola, consumidor, reintento,
                                error.getMessage()));

            } catch (Exception falloReintento) {
                // Si no se pudo publicar el reintento, mandamos el original a DLQ.
                manejarPermanente(message, channel, deliveryTag, pedido, messageId,
                        cola, consumidor, reintento,
                        new ErrorPermanenteException(
                                "No se pudo programar el reintento: "
                                        + falloReintento.getMessage(),
                                falloReintento));
            }
        } else {
            log.warn("Reintentos agotados ({}/{}): messageId={} -> DLQ",
                    reintento, max, messageId);

            marcar(pedido, "FALLIDO", reintento + 1,
                    "Reintentos agotados: " + error.getMessage());
            anotarError(message, "Reintentos agotados: " + error.getMessage(), reintento + 1);

            channel.basicNack(deliveryTag, false, false);

            eventos.publicar("REINTENTOS_AGOTADOS",
                    "Pedido #" + pedido.getPedidoId()
                            + " agoto " + max + " reintento(s) -> DLQ",
                    datos(pedido, messageId, cola, consumidor, reintento,
                            error.getMessage()));
        }
    }

    private Pedido leerPedido(Message message) {
        try {
            byte[] body = message.getBody();
            if (body == null || body.length == 0) {
                throw new ErrorPermanenteException("El mensaje esta vacio");
            }
            Pedido pedido = objectMapper.readValue(body, Pedido.class);
            if (pedido == null) {
                throw new ErrorPermanenteException("El mensaje no contiene un pedido");
            }
            if (pedido.getEstado() == null) {
                pedido.setEstado("PENDIENTE");
            }
            if (pedido.getIntentos() == null) {
                pedido.setIntentos(0);
            }
            return pedido;
        } catch (ErrorPermanenteException e) {
            throw e;
        } catch (Exception e) {
            String contenido = message.getBody() == null
                    ? ""
                    : new String(message.getBody(), StandardCharsets.UTF_8);
            throw new ErrorPermanenteException(
                    "JSON de pedido invalido. Contenido: "
                            + recortar(contenido, 180), e);
        }
    }

    private void validarPedido(Pedido pedido) {
        validator.validar(pedido);
    }

    private String asegurarMessageId(Message message) {
        String messageId = message.getMessageProperties().getMessageId();
        if (messageId == null || messageId.isBlank()) {
            messageId = "auto-" + UUID.randomUUID();
            message.getMessageProperties().setMessageId(messageId);
        }
        return messageId;
    }

    private boolean esReprocesado(Message message) {
        Object valor = message.getMessageProperties().getHeaders().get("x-reprocesado");
        if (valor instanceof Boolean booleano) {
            return booleano;
        }
        return valor != null && Boolean.parseBoolean(valor.toString());
    }

    private int leerReintento(Message message) {
        Map<String, Object> headers = message.getMessageProperties().getHeaders();
        int valor = leerEntero(headers.get(RabbitMQConfig.HEADER_REINTENTOS));
        if (valor == 0) {
            valor = contarReintentosXDeath(headers);
        }
        return Math.max(0, valor);
    }

    private int contarReintentosXDeath(Map<String, Object> headers) {
        Object xDeath = headers.get("x-death");
        if (!(xDeath instanceof List<?> lista)) {
            return 0;
        }
        int total = 0;
        for (Object item : lista) {
            if (item instanceof Map<?, ?> mapa) {
                Object cola = mapa.get("queue");
                Object count = mapa.get("count");
                if (cola != null
                        && RabbitMQConfig.COLA_REINTENTOS.equals(cola.toString())) {
                    total += leerEntero(count);
                }
            }
        }
        return total;
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

    private void anotarError(Message message, String motivo, int intentos) {
        message.getMessageProperties().setHeader(
                RabbitMQConfig.HEADER_ULTIMO_ERROR, motivo
        );
        message.getMessageProperties().setHeader(
                RabbitMQConfig.HEADER_INTENTOS, intentos
        );
    }

    private void marcar(Pedido pedido, String estado, int intentos, String error) {
        if (pedido == null || pedido.getPedidoId() == null) {
            return;
        }

        // Si el mensaje se publico directo a RabbitMQ (sin pasar por la API),
        // lo incorporamos a la tabla de pedidos para que la demo sea completa.
        if (pedidos.obtener(pedido.getPedidoId()) == null) {
            Pedido nuevo = pedido.copia();
            if (nuevo.getFechaCreacion() == null) {
                nuevo.setFechaCreacion(System.currentTimeMillis());
            }
            nuevo.setEstado(estado);
            nuevo.setIntentos(intentos);
            nuevo.setUltimoError(error);
            pedidos.guardar(nuevo);
            return;
        }

        pedidos.actualizar(pedido.getPedidoId(), actual -> {
            actual.setEstado(estado);
            actual.setIntentos(intentos);
            actual.setUltimoError(error);
            if (pedido.getMessageId() != null) {
                actual.setMessageId(pedido.getMessageId());
            }
        });
    }

    private Map<String, Object> datos(
            Pedido pedido,
            String messageId,
            String cola,
            String consumidor,
            int reintento,
            String error) {

        PoliticaReintentos politica = politicaService.getPolitica();

        return EventoService.datos(
                "pedidoId", pedido == null ? null : pedido.getPedidoId(),
                "messageId", messageId,
                "cliente", pedido == null ? null : pedido.getCliente(),
                "producto", pedido == null ? null : pedido.getProducto(),
                "cantidad", pedido == null ? null : pedido.getCantidad(),
                "estado", pedido == null ? null : pedido.getEstado(),
                "tipoError", pedido == null ? null : pedido.getTipoError(),
                "cola", cola,
                "consumidor", consumidor,
                "intento", reintento + 1,
                "maxReintentos", politica.maxReintentos(),
                "error", error
        );
    }

    /**
     * Los contenedores de @RabbitListener usan nombres largos
     * (org.springframework...Container#0-1). Para el monitor basta el sufijo.
     */
    private String nombreConsumidor() {
        String nombre = Thread.currentThread().getName();
        int indice = nombre.lastIndexOf('#');
        return indice >= 0 ? nombre.substring(indice) : nombre;
    }

    private void dormir(long milisegundos) throws InterruptedException {
        if (milisegundos > 0) {
            Thread.sleep(milisegundos);
        }
    }

    private String recortar(String texto, int maximo) {
        if (texto == null || texto.length() <= maximo) {
            return texto;
        }
        return texto.substring(0, maximo) + "...";
    }

    /** Visible para pruebas unitarias. */
    public boolean fueProcesado(String messageId) {
        return mensajesProcesados.contains(messageId);
    }
}
