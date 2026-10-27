package com.minipedidos.service;

import com.minipedidos.model.Pedido;
import org.springframework.stereotype.Service;

import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Almacen en memoria de pedidos para la demo. Permite ver el ciclo de vida del
 * pedido (PENDIENTE, PROCESANDO, EN_REINTENTO, PROCESADO, FALLIDO) sin
 * incorporar una base de datos. Al reiniciar la aplicacion se limpia.
 */
@Service
public class PedidoStore {

    private final Map<Long, Pedido> pedidos = new ConcurrentHashMap<>();
    private final AtomicLong secuencia = new AtomicLong(1000);

    public long siguienteId() {
        return secuencia.incrementAndGet();
    }

    public void guardar(Pedido pedido) {
        if (pedido == null || pedido.getPedidoId() == null) {
            return;
        }
        pedidos.put(pedido.getPedidoId(), pedido.copia());
    }

    public Pedido obtener(Long pedidoId) {
        if (pedidoId == null) {
            return null;
        }
        Pedido pedido = pedidos.get(pedidoId);
        return pedido == null ? null : pedido.copia();
    }

    public void actualizar(Long pedidoId, java.util.function.Consumer<Pedido> mutador) {
        if (pedidoId == null) {
            return;
        }
        pedidos.computeIfPresent(pedidoId, (id, actual) -> {
            Pedido copia = actual.copia();
            mutador.accept(copia);
            copia.setFechaActualizacion(System.currentTimeMillis());
            return copia;
        });
    }

    public List<Pedido> listar() {
        return pedidos.values().stream()
                .map(Pedido::copia)
                .sorted(Comparator.comparing(
                        (Pedido p) -> p.getFechaCreacion() == null ? 0L : p.getFechaCreacion()
                ).reversed())
                .limit(60)
                .toList();
    }

    public Map<String, Long> resumen() {
        Map<String, Long> resumen = new LinkedHashMap<>();
        long total = pedidos.size();
        long procesados = contar("PROCESADO");
        long fallidos = contar("FALLIDO") + contar("ERROR_ENVIO");
        long enReintento = contar("EN_REINTENTO");
        long pendientes = contar("PENDIENTE") + contar("PROCESANDO");
        long duplicados = contar("DUPLICADO");

        resumen.put("total", total);
        resumen.put("procesados", procesados);
        resumen.put("fallidos", fallidos);
        resumen.put("enReintento", enReintento);
        resumen.put("pendientes", pendientes);
        resumen.put("duplicados", duplicados);
        return resumen;
    }

    private long contar(String estado) {
        return pedidos.values().stream()
                .filter(p -> estado.equalsIgnoreCase(p.getEstado()))
                .count();
    }
}
