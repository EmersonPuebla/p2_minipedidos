package com.minipedidos.service;

import com.minipedidos.dto.CrearBindingRequest;
import com.minipedidos.dto.CrearColaRequest;
import com.minipedidos.dto.CrearExchangeRequest;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.amqp.core.AmqpAdmin;
import org.springframework.amqp.core.Binding;
import org.springframework.amqp.core.Exchange;
import org.springframework.amqp.core.Queue;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class RabbitAdminServiceTest {

    @Mock
    private AmqpAdmin amqpAdmin;

    @Mock
    private EventoService eventos;

    @InjectMocks
    private RabbitAdminService service;

    @Test
    void creaUnaColaSimple() {
        service.crearCola(new CrearColaRequest("demo.cola", true, null, null, null));

        verify(amqpAdmin, times(1)).declareQueue(any(Queue.class));
        verify(amqpAdmin, never()).declareExchange(any(Exchange.class));
        verify(amqpAdmin, never()).declareBinding(any(Binding.class));
    }

    @Test
    void creaUnaColaConDlxYdlq() {
        service.crearCola(new CrearColaRequest(
                "demo.cola", true, "demo.dlx", "demo.error", "demo.dlq"));

        verify(amqpAdmin, times(1)).declareExchange(any(Exchange.class));
        verify(amqpAdmin, times(2)).declareQueue(any(Queue.class));
        verify(amqpAdmin, times(1)).declareBinding(any(Binding.class));
    }

    @Test
    void rechazaDlxIncompleto() {
        assertThrows(IllegalArgumentException.class, () -> service.crearCola(
                new CrearColaRequest("demo.cola", true, "demo.dlx", null, null)));
    }

    @Test
    void eliminaUnaCola() {
        when(amqpAdmin.deleteQueue("demo.cola")).thenReturn(true);

        assertTrue(service.eliminarCola("demo.cola"));
        verify(amqpAdmin).deleteQueue("demo.cola");
    }

    @Test
    void creaUnExchangeDirecto() {
        service.crearExchange(new CrearExchangeRequest("demo.exchange", "direct", true));

        verify(amqpAdmin).declareExchange(any(Exchange.class));
    }

    @Test
    void creaUnBinding() {
        service.crearBinding(new CrearBindingRequest(
                "demo.exchange", "demo.cola", "demo.nuevo"));

        verify(amqpAdmin).declareBinding(any(Binding.class));
    }

    @Test
    void eliminaUnBinding() {
        service.eliminarBinding(new CrearBindingRequest(
                "demo.exchange", "demo.cola", "demo.nuevo"));

        verify(amqpAdmin).removeBinding(any(Binding.class));
    }
}
