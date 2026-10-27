package com.minipedidos.service;

import com.minipedidos.exception.ErrorPermanenteException;
import com.minipedidos.model.Pedido;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PedidoValidatorTest {

    private final PedidoValidator validator = new PedidoValidator();

    @Test
    void aceptaUnPedidoValido() {
        assertDoesNotThrow(() -> validator.validar(
                new Pedido(1L, "Ana", "Notebook", 2)));
    }

    @Test
    void rechazaPedidoNulo() {
        assertThrows(ErrorPermanenteException.class, () -> validator.validar(null));
    }

    @Test
    void rechazaClienteVacio() {
        assertThrows(ErrorPermanenteException.class, () -> validator.validar(
                new Pedido(1L, "  ", "Notebook", 2)));
    }

    @Test
    void rechazaProductoVacio() {
        assertThrows(ErrorPermanenteException.class, () -> validator.validar(
                new Pedido(1L, "Ana", "", 2)));
    }

    @Test
    void rechazaCantidadCeroONegativa() {
        assertThrows(ErrorPermanenteException.class, () -> validator.validar(
                new Pedido(1L, "Ana", "Notebook", 0)));
        assertThrows(ErrorPermanenteException.class, () -> validator.validar(
                new Pedido(1L, "Ana", "Notebook", -2)));
    }

    @Test
    void rechazaCantidadExcesiva() {
        assertThrows(ErrorPermanenteException.class, () -> validator.validar(
                new Pedido(1L, "Ana", "Notebook", 10_001)));
    }
}
