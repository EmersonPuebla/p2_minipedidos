package com.minipedidos.service;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.model.Topologia;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.BindingBuilder;
import org.springframework.amqp.core.DirectExchange;
import org.springframework.amqp.core.Queue;
import org.springframework.amqp.core.QueueBuilder;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.context.event.ApplicationReadyEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Service;

/**
 * Declara la topologia del laboratorio de forma idempotente.
 *
 * Los beans de Queue/Exchange/Binding de {@link RabbitMQConfig} son la
 * declaracion principal. Este servicio vuelve a declarar al arrancar para
 * asegurar que la topologia exista antes de que el consumidor procese, y
 * publica un evento para el monitor de la UI.
 */
@Service
public class TopologiaService {

    private static final Logger log = LoggerFactory.getLogger(TopologiaService.class);

    private final AmqpAdmin amqpAdmin;
    private final EventoService eventos;
    private final boolean iniciarConsumidores;
    private final Topologia topologia = Topologia.porDefecto();

    private volatile boolean conectado;

    public TopologiaService(
            AmqpAdmin amqpAdmin,
            EventoService eventos,
            @Value("${demo.iniciar-consumidores:true}") boolean iniciarConsumidores) {

        this.amqpAdmin = amqpAdmin;
        this.eventos = eventos;
        this.iniciarConsumidores = iniciarConsumidores;
    }

    @EventListener(ApplicationReadyEvent.class)
    public void alIniciar() {
        if (!iniciarConsumidores) {
            log.info("Consumidores desactivados por configuracion (demo.iniciar-consumidores=false)");
            return;
        }
        inicializar();
    }

    /**
     * Declara la topologia. Es idempotente y se puede invocar desde el boton
     * "Reconectar" del formulario.
     */
    public synchronized boolean inicializar() {
        if (!iniciarConsumidores) {
            return false;
        }

        try {
            declararFijos();
            declararTopologia(topologia);
            conectado = true;

            eventos.publicar("TOPOLOGIA_LISTA",
                    "Topologia declarada y consumidores activos",
                    EventoService.datos(
                            "colaPrincipal", topologia.colaPrincipal(),
                            "exchangePrincipal", RabbitMQConfig.EXCHANGE_PEDIDOS,
                            "dlx", topologia.exchangeDlx(),
                            "dlq", topologia.colaDlq()
                    ));

            return true;
        } catch (Exception e) {
            conectado = false;
            log.error("No se pudo inicializar RabbitMQ: {}", e.getMessage());
            eventos.publicar("RABBITMQ_ERROR",
                    "No se pudo inicializar RabbitMQ: " + e.getMessage(),
                    EventoService.datos(
                            "sugerencia",
                            "Verifica que RabbitMQ este iniciado (docker compose up -d)"
                    ));
            return false;
        }
    }

    private void declararFijos() {
        DirectExchange principal = RabbitMQConfig.exchangePrincipal();
        DirectExchange reintentos = RabbitMQConfig.exchangeReintentos();
        Queue colaReintentos = RabbitMQConfig.colaReintentos();

        amqpAdmin.declareExchange(principal);
        amqpAdmin.declareExchange(reintentos);
        amqpAdmin.declareQueue(colaReintentos);
        amqpAdmin.declareBinding(BindingBuilder
                .bind(colaReintentos)
                .to(reintentos)
                .with(RabbitMQConfig.ROUTING_KEY_REINTENTOS));

        log.info("Topologia fija declarada: exchange={}, retryExchange={}, retryQueue={}",
                RabbitMQConfig.EXCHANGE_PEDIDOS,
                RabbitMQConfig.EXCHANGE_REINTENTOS,
                RabbitMQConfig.COLA_REINTENTOS);
    }

    private void declararTopologia(Topologia configuracion) {
        DirectExchange dlx = new DirectExchange(
                configuracion.exchangeDlx(), true, false
        );

        Queue principal = QueueBuilder.durable(configuracion.colaPrincipal())
                .deadLetterExchange(configuracion.exchangeDlx())
                .deadLetterRoutingKey(configuracion.routingKeyDlx())
                .build();

        Queue dlq = QueueBuilder.durable(configuracion.colaDlq()).build();

        amqpAdmin.declareExchange(dlx);
        amqpAdmin.declareQueue(principal);
        amqpAdmin.declareQueue(dlq);

        amqpAdmin.declareBinding(BindingBuilder
                .bind(principal)
                .to(new DirectExchange(RabbitMQConfig.EXCHANGE_PEDIDOS, true, false))
                .with(RabbitMQConfig.ROUTING_KEY));

        amqpAdmin.declareBinding(BindingBuilder
                .bind(dlq)
                .to(dlx)
                .with(configuracion.routingKeyDlx()));

        log.info("Topologia declarada: cola={}, dlx={}, routingKeyError={}, dlq={}",
                configuracion.colaPrincipal(),
                configuracion.exchangeDlx(),
                configuracion.routingKeyDlx(),
                configuracion.colaDlq());
    }

    public Topologia getTopologia() {
        return topologia;
    }

    public boolean isConectado() {
        return conectado;
    }
}
