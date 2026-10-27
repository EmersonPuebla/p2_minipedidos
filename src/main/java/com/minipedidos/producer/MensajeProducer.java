package com.minipedidos.producer;

import tools.jackson.databind.ObjectMapper;
import com.minipedidos.service.EventoService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.core.Message;
import org.springframework.amqp.core.MessageDeliveryMode;
import org.springframework.amqp.core.MessageProperties;
import org.springframework.amqp.rabbit.core.RabbitTemplate;
import org.springframework.stereotype.Service;

import java.nio.charset.StandardCharsets;
import java.util.UUID;

/**
 * Publicacion libre de mensajes para demostrar exchanges, routing keys y
 * mensajes "veneno" que no cumplen el contrato de Pedido.
 */
@Service
public class MensajeProducer {

    private static final Logger log = LoggerFactory.getLogger(MensajeProducer.class);

    private final RabbitTemplate rabbitTemplate;
    private final ObjectMapper objectMapper;
    private final EventoService eventos;

    public MensajeProducer(
            RabbitTemplate rabbitTemplate,
            ObjectMapper objectMapper,
            EventoService eventos) {

        this.rabbitTemplate = rabbitTemplate;
        this.objectMapper = objectMapper;
        this.eventos = eventos;
    }

    public String publicar(String exchange, String routingKey, String contenido) {
        String messageId = "msg-" + UUID.randomUUID();

        MessageProperties props = new MessageProperties();
        props.setMessageId(messageId);
        props.setDeliveryMode(MessageDeliveryMode.PERSISTENT);
        props.setContentType(tipoContenido(contenido));
        props.setContentEncoding(StandardCharsets.UTF_8.name());
        props.setHeader("x-origen", "formulario");
        props.setTimestamp(new java.util.Date());

        Message message = new Message(contenido.getBytes(StandardCharsets.UTF_8), props);
        rabbitTemplate.send(exchange, routingKey, message);

        log.info("Mensaje publicado: exchange={}, routingKey={}, messageId={}",
                exchange, routingKey, messageId);

        eventos.publicar("MENSAJE_PUBLICADO",
                "Mensaje publicado en " + exchange + " con routing key " + routingKey,
                EventoService.datos(
                        "messageId", messageId,
                        "exchange", exchange,
                        "routingKey", routingKey,
                        "contenido", contenido.length() > 300
                                ? contenido.substring(0, 300) + "..."
                                : contenido
                ));

        return messageId;
    }

    private String tipoContenido(String contenido) {
        try {
            objectMapper.readTree(contenido);
            return MessageProperties.CONTENT_TYPE_JSON;
        } catch (Exception e) {
            return MessageProperties.CONTENT_TYPE_TEXT_PLAIN;
        }
    }
}
