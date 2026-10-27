package com.minipedidos.producer;

import com.minipedidos.config.RabbitMQConfig;
import com.minipedidos.model.Pedido;
import com.minipedidos.service.EventoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessagePostProcessor;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.util.UUID;

@Service
public class PedidoProducer {

    private static final Logger log = LoggerFactory.getLogger(PedidoProducer.class);

    private final RabbitTemplate rabbitTemplate;
    private final EventoService eventos;

    public PedidoProducer(RabbitTemplate rabbitTemplate, EventoService eventos) {
        this.rabbitTemplate = rabbitTemplate;
        this.eventos = eventos;
    }

    /**
     * Publica el pedido y devuelve el messageId generado. El messageId se usa
     * para idempotencia y para poder identificar el mensaje en la DLQ.
     */
    public String enviarPedido(Pedido pedido) {
        String messageId = "pedido-" + pedido.getPedidoId() + "-"
                + UUID.randomUUID().toString().substring(0, 8);

        // Se asigna antes de convertir para que el JSON tambien lleve el id.
        pedido.setMessageId(messageId);

        MessagePostProcessor postProcessor = message -> {
            message.getMessageProperties().setMessageId(messageId);
            message.getMessageProperties().setContentType("application/json");
            message.getMessageProperties().setDeliveryMode(MessageDeliveryMode.PERSISTENT);
            message.getMessageProperties().setHeader(RabbitMQConfig.HEADER_ORIGEN, "api");
            message.getMessageProperties().setHeader("x-pedido-id", pedido.getPedidoId());
            return message;
        };

        rabbitTemplate.convertAndSend(
                RabbitMQConfig.EXCHANGE_PEDIDOS,
                RabbitMQConfig.ROUTING_KEY,
                pedido,
                postProcessor
        );

        log.info("Pedido enviado a RabbitMQ: id={}, messageId={}, cliente={}",
                pedido.getPedidoId(), messageId, pedido.getCliente());

        eventos.publicar("PEDIDO_PUBLICADO", "Pedido #" + pedido.getPedidoId()
                        + " publicado en " + RabbitMQConfig.EXCHANGE_PEDIDOS,
                EventoService.datos(
                        "pedidoId", pedido.getPedidoId(),
                        "messageId", messageId,
                        "cliente", pedido.getCliente(),
                        "producto", pedido.getProducto(),
                        "cantidad", pedido.getCantidad(),
                        "exchange", RabbitMQConfig.EXCHANGE_PEDIDOS,
                        "routingKey", RabbitMQConfig.ROUTING_KEY
                ));

        return messageId;
    }
}
