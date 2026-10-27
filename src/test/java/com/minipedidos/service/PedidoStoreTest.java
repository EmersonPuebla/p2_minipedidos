package com.minipedidos.service;

import com.minipedidos.model.Pedido;
import org.junit.jupiter.api.Test;

import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

class PedidoStoreTest {

    @Test
    void generaIdsConsecutivos() {
        PedidoStore store = new PedidoStore();

        assertEquals(1001, store.siguienteId());
        assertEquals(1002, store.siguienteId());
    }

    @Test
    void guardaYDevuelveCopias() {
        PedidoStore store = new PedidoStore();
        Pedido pedido = new Pedido(1001L, "Ana", "Notebook", 1);
        pedido.setEstado("PENDIENTE");

        store.guardar(pedido);
        Pedido recuperado = store.obtener(1001L);

        assertEquals("Ana", recuperado.getCliente());
        assertNotSame(pedido, recuperado);
    }

    @Test
    void actualizaElEstado() {
        PedidoStore store = new PedidoStore();
        Pedido pedido = new Pedido(1001L, "Ana", "Notebook", 1);
        pedido.setEstado("PENDIENTE");
        store.guardar(pedido);

        store.actualizar(1001L, actual -> {
            actual.setEstado("PROCESADO");
            actual.setIntentos(2);
        });

        Pedido recuperado = store.obtener(1001L);
        assertEquals("PROCESADO", recuperado.getEstado());
        assertEquals(2, recuperado.getIntentos());
    }

    @Test
    void resumeLosEstados() {
        PedidoStore store = new PedidoStore();
        for (int i = 0; i < 3; i++) {
            Pedido pedido = new Pedido((long) (1001 + i), "Ana", "Notebook", 1);
            pedido.setEstado(i == 0 ? "PROCESADO" : i == 1 ? "FALLIDO" : "PENDIENTE");
            store.guardar(pedido);
        }

        Map<String, Long> resumen = store.resumen();

        assertEquals(3L, resumen.get("total"));
        assertEquals(1L, resumen.get("procesados"));
        assertEquals(1L, resumen.get("fallidos"));
        assertEquals(1L, resumen.get("pendientes"));
        assertTrue(resumen.containsKey("enReintento"));
    }
}
