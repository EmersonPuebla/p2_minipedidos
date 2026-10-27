package com.minipedidos.service;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.model.Pedido;
import com.minipedidos.model.PoliticaReintentos;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

/**
 * Publica una copia del pedido en la cola de reintentos.
 *
 * La cola {@code pedidos.retry.cola} tiene un TTL por mensaje y un
 * dead-letter-exchange que apunta al exchange principal. Cuando el TTL expira,
 * RabbitMQ reencola el mensaje en la cola principal. Asi no bloqueamos el hilo
 * del consumidor y el reintento sobrevive a reinicios de la aplicacion.
 */
@Service
public class ReintentoService {

    private static final Logger log = LoggerFactory.getLogger(ReintentoService.class);

    private final RabbitTemplate rabbitTemplate;
    private final PoliticaReintentosService politicaService;
    private final EventoService eventos;

    public ReintentoService(
            RabbitTemplate rabbitTemplate,
            PoliticaReintentosService politicaService,
            EventoService eventos) {

        this.rabbitTemplate = rabbitTemplate;
        this.politicaService = politicaService;
        this.eventos = eventos;
    }

    public void programar(Pedido pedido, Message original, int siguienteIntento) {
        PoliticaReintentos politica = politicaService.getPolitica();
        String messageId = original.getMessageProperties().getMessageId();

        // Mantiene el messageId en el cuerpo para que el reintento sea
        // autocontenido y la idempotencia funcione igual.
        pedido.setMessageId(messageId);

        MessagePostProcessor postProcessor = message -> {
            message.getMessageProperties().setMessageId(messageId);
            message.getMessageProperties().setContentType("application/json");
            message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            message.getMessageProperties().setExpiration(
                    String.valueOf(politica.intervaloMs())
            );
            message.getMessageProperties().setHeader(
                    RabbitMQConfig.HEADER_REINTENTOS, siguienteIntento
            );
            message.getMessageProperties().setHeader(
                    RabbitMQConfig.HEADER_ORIGEN, "reintento"
            );
            message.getMessageProperties().setHeader(
                    "x-pedido-id", pedido.getPedidoId()
            );
            return message;
        };

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.EXCHANGE_REINTENTOS,
                RabbitMQConfig.ROUTING_KEY_REINTENTOS,
                pedido,
                postProcessor
        );

        log.info("Reintento programado: pedido={}, intento={}, espera={}s, messageId={}",
                pedido.getPedidoId(), siguienteIntento,
                politica.intervaloSegundos(), messageId);

        eventos.publicar("REINTENTO_PROGRAMADO",
                "Pedido #" + pedido.getPedidoId() + " se reintentara en "
                        + politica.intervaloSegundos() + " s (intento "
                        + siguienteIntento + ")",
                EventoService.datos(
                        "pedidoId", pedido.getPedidoId(),
                        "messageId", messageId,
                        "intento", siguienteIntento,
                        "maxReintentos", politica.maxReintentos(),
                        "intervaloSegundos", politica.intervaloSegundos(),
                        "colaReintentos", RabbitMQConfig.COLA_REINTENTOS
                ));
    }
}
