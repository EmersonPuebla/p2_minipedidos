package com.minipedidos.service;

import com.minipedidos.dto.CrearBindingRequest;
import com.minipedidos.dto.CrearColaRequest;
import com.minipedidos.dto.CrearExchangeRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.ExchangeBuilder;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.stereotype.Service;

/**
 * Administracion generica de colas, exchanges y bindings.
 *
 * Es el servicio dedicado que pide la pauta: los controladores no conocen
 * AmqpAdmin ni los detalles de RabbitMQ. Toda la logica de declaracion y
 * eliminacion queda aqui.
 */
@Service
public class RabbitAdminService {

    private static final Logger log = LoggerFactory.getLogger(RabbitAdminService.class);

    private final AmqpAdmin amqpAdmin;
    private final EventoService eventos;

    public RabbitAdminService(AmqpAdmin amqpAdmin, EventoService eventos) {
        this.amqpAdmin = amqpAdmin;
        this.eventos = eventos;
    }

    public void crearCola(CrearColaRequest request) {
        QueueBuilder builder = request.durableSeguro()
                ? QueueBuilder.durable(request.nombre())
                : QueueBuilder.nonDurable(request.nombre());

        if (request.tieneDlx()) {
            if (vacio(request.dlx())
                    || vacio(request.routingKeyDlx())
                    || vacio(request.dlq())) {
                throw new IllegalArgumentException(
                        "Para configurar DLX/DLQ debes indicar dlx, routingKeyDlx y dlq"
                );
            }

            DirectExchange dlx = new DirectExchange(request.dlx(), true, false);
            Queue dlq = QueueBuilder.durable(request.dlq()).build();

            amqpAdmin.declareExchange(dlx);
            amqpAdmin.declareQueue(dlq);
            amqpAdmin.declareBinding(BindingBuilder
                    .bind(dlq)
                    .to(dlx)
                    .with(request.routingKeyDlx()));

            builder.deadLetterExchange(request.dlx())
                    .deadLetterRoutingKey(request.routingKeyDlx());
        }

        amqpAdmin.declareQueue(builder.build());

        log.info("Cola creada: {}", request.nombre());
        eventos.publicar("ADMIN_COLA_CREADA",
                "Cola " + request.nombre() + " creada",
                EventoService.datos(
                        "cola", request.nombre(),
                        "durable", request.durableSeguro(),
                        "dlx", request.dlx(),
                        "routingKeyDlx", request.routingKeyDlx(),
                        "dlq", request.dlq()
                ));
    }

    public boolean eliminarCola(String nombre) {
        validarNombre(nombre);
        boolean eliminada = amqpAdmin.deleteQueue(nombre);
        log.info("Cola eliminada: {} ({})", nombre, eliminada);
        eventos.publicar("ADMIN_COLA_ELIMINADA",
                "Cola " + nombre + (eliminada ? " eliminada" : " no encontrada"),
                EventoService.datos("cola", nombre, "eliminada", eliminada));
        return eliminada;
    }

    public void crearExchange(CrearExchangeRequest request) {
        boolean durable = request.durableSeguro();

        Exchange exchange = switch (request.tipo().toLowerCase()) {
            case "direct" -> ExchangeBuilder.directExchange(request.nombre())
                    .durable(durable).build();
            case "fanout" -> ExchangeBuilder.fanoutExchange(request.nombre())
                    .durable(durable).build();
            case "topic" -> ExchangeBuilder.topicExchange(request.nombre())
                    .durable(durable).build();
            case "headers" -> ExchangeBuilder.headersExchange(request.nombre())
                    .durable(durable).build();
            default -> throw new IllegalArgumentException(
                    "Tipo de exchange no soportado: " + request.tipo());
        };

        amqpAdmin.declareExchange(exchange);

        log.info("Exchange creado: {} ({})", request.nombre(), request.tipo());
        eventos.publicar("ADMIN_EXCHANGE_CREADO",
                "Exchange " + request.nombre() + " creado",
                EventoService.datos(
                        "exchange", request.nombre(),
                        "tipo", request.tipo(),
                        "durable", durable
                ));
    }

    public boolean eliminarExchange(String nombre) {
        validarNombre(nombre);
        boolean eliminado = amqpAdmin.deleteExchange(nombre);
        log.info("Exchange eliminado: {} ({})", nombre, eliminado);
        eventos.publicar("ADMIN_EXCHANGE_ELIMINADO",
                "Exchange " + nombre + (eliminado ? " eliminado" : " no encontrado"),
                EventoService.datos("exchange", nombre, "eliminado", eliminado));
        return eliminado;
    }

    public void crearBinding(CrearBindingRequest request) {
        amqpAdmin.declareBinding(BindingBuilder
                .bind(new Queue(request.cola()))
                .to(new DirectExchange(request.exchange()))
                .with(request.routingKey()));

        log.info("Binding creado: {} -> {} ({})",
                request.exchange(), request.cola(), request.routingKey());

        eventos.publicar("ADMIN_BINDING_CREADO",
                "Binding " + request.exchange() + " -> " + request.cola()
                        + " con routing key " + request.routingKey(),
                EventoService.datos(
                        "exchange", request.exchange(),
                        "cola", request.cola(),
                        "routingKey", request.routingKey()
                ));
    }

    public boolean eliminarBinding(CrearBindingRequest request) {
        amqpAdmin.removeBinding(new Binding(
                request.cola(),
                Binding.DestinationType.QUEUE,
                request.exchange(),
                request.routingKey(),
                null
        ));

        log.info("Binding eliminado: {} -> {} ({})",
                request.exchange(), request.cola(), request.routingKey());

        eventos.publicar("ADMIN_BINDING_ELIMINADO",
                "Binding " + request.exchange() + " -> " + request.cola()
                        + " con routing key " + request.routingKey() + " eliminado",
                EventoService.datos(
                        "exchange", request.exchange(),
                        "cola", request.cola(),
                        "routingKey", request.routingKey()
                ));

        return true;
    }

    private void validarNombre(String nombre) {
        if (vacio(nombre) || !nombre.matches("[A-Za-z0-9._-]{1,120}")) {
            throw new IllegalArgumentException("Nombre invalido: " + nombre);
        }
    }

    private boolean vacio(String valor) {
        return valor == null || valor.isBlank();
    }
}
