package com.minipedidos.service;

import com.minipedidos.exception.ErrorPermanenteException;
import com.minipedidos.exception.ErrorTemporalException;
import com.minipedidos.model.Pedido;
import org.junit.jupiter.api.Test;

import java.util.List;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

class SimuladorErroresTest {

    private final SimuladorErrores simulador = new SimuladorErrores();

    private Pedido pedido(String cliente, String tipoError) {
        Pedido pedido = new Pedido(1L, cliente, "Notebook", 1);
        pedido.setTipoError(tipoError);
        return pedido;
    }

    @Test
    void unPedidoNormalNoFalla() {
        assertDoesNotThrow(() -> simulador.evaluar(pedido("Ana", null), 0));
    }

    @Test
    void rechazadoEsPermanente() {
        assertThrows(ErrorPermanenteException.class,
                () -> simulador.evaluar(pedido("Ana", "rechazado"), 0));
    }

    @Test
    void falloEsPermanente() {
        assertThrows(ErrorPermanenteException.class,
                () -> simulador.evaluar(pedido("Ana", "fallo"), 0));
    }

    @Test
    void requeueFallaLaPrimeraVezYSeRecupera() {
        Pedido pedido = pedido("Ana", "requeue");

        assertThrows(ErrorTemporalException.class,
                () -> simulador.evaluar(pedido, 0));
        assertDoesNotThrow(() -> simulador.evaluar(pedido, 1));
    }

    @Test
    void timeoutFallaSiempre() {
        assertThrows(ErrorTemporalException.class,
                () -> simulador.evaluar(pedido("Ana", "timeout"), 0));
        assertThrows(ErrorTemporalException.class,
                () -> simulador.evaluar(pedido("Ana", "timeout"), 5));
    }

    @Test
    void clienteErrorEsPermanenteSinImportarMayusculas() {
        List<String> clientes = List.of(
                "error",
                "ERROR",
                "ErRoR",
                "  error  "
        );

        for (String cliente : clientes) {
            assertThrows(ErrorPermanenteException.class,
                    () -> simulador.evaluar(pedido(cliente, null), 0),
                    "Debe fallar para el cliente: " + cliente);
        }
    }

    @Test
    void clienteParecidoAErrorNoFalla() {
        assertDoesNotThrow(
                () -> simulador.evaluar(pedido("Errorcito", null), 0));
    }

    @Test
    void clienteErrorTienePrioridadSobreOtraAccion() {
        assertThrows(ErrorPermanenteException.class,
                () -> simulador.evaluar(pedido("error", "requeue"), 0));
    }

    @Test
    void dlqRecuperableFallaHastaQueSeReprocesa() {
        Pedido pedido = pedido("Ana", "dlq-recuperable");

        assertThrows(ErrorPermanenteException.class,
                () -> simulador.evaluar(pedido, 0, false));
        assertDoesNotThrow(() -> simulador.evaluar(pedido, 0, true));
    }

    @Test
    void clienteTemporalSeRecupera() {
        Pedido pedido = pedido("TEMPORAL", null);

        assertThrows(ErrorTemporalException.class,
                () -> simulador.evaluar(pedido, 0));
        assertDoesNotThrow(() -> simulador.evaluar(pedido, 1));
    }
}
