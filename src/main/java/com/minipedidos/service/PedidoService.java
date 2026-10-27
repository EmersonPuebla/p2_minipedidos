package com.minipedidos.service;

import com.minipedidos.model.Pedido;
import com.minipedidos.producer.PedidoProducer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;

/**
 * Orquesta la creacion de pedidos: asigna ID, guarda la vista en memoria y
 * publica el mensaje. Tambien genera pedidos de demostracion y de error.
 */
@Service
public class PedidoService {

    private static final Logger log = LoggerFactory.getLogger(PedidoService.class);

    private static final List<String> CLIENTES_DEMO = List.of(
            "Ana", "Pedro", "Maria", "Sofia", "Luis", "Carlos",
            "Valentina", "Diego", "Camila", "Javier"
    );

    private static final List<String> PRODUCTOS_DEMO = List.of(
            "Notebook", "Monitor", "Teclado", "Mouse", "Auriculares", "Webcam"
    );

    private final PedidoStore store;
    private final PedidoProducer producer;
    private final EventoService eventos;

    public PedidoService(
            PedidoStore store,
            PedidoProducer producer,
            EventoService eventos) {

        this.store = store;
        this.producer = producer;
        this.eventos = eventos;
    }

    public Pedido crear(Pedido pedido) {
        long id = store.siguienteId();
        long ahora = System.currentTimeMillis();

        pedido.setPedidoId(id);
        pedido.setEstado("PENDIENTE");
        pedido.setIntentos(0);
        pedido.setUltimoError(null);
        pedido.setFechaCreacion(ahora);
        pedido.setFechaActualizacion(ahora);

        store.guardar(pedido);

        try {
            String messageId = producer.enviarPedido(pedido);
            pedido.setMessageId(messageId);
            store.guardar(pedido);
            return pedido.copia();
        } catch (RuntimeException e) {
            store.actualizar(id, actual -> {
                actual.setEstado("ERROR_ENVIO");
                actual.setUltimoError("No se pudo publicar en RabbitMQ: " + e.getMessage());
            });
            log.error("No se pudo publicar el pedido {}: {}", id, e.getMessage());
            throw e;
        }
    }

    /**
     * Si la referencia es un ID de pedido existente, clona ese pedido; si no,
     * crea un pedido sintetico usando la referencia como cliente.
     */
    public Pedido simular(String referencia, String tipoError) {
        Pedido base = buscarPorId(referencia);

        Pedido pedido = new Pedido();
        if (base != null) {
            pedido.setCliente(base.getCliente());
            pedido.setProducto(base.getProducto());
            pedido.setCantidad(base.getCantidad());
        } else {
            pedido.setCliente(referencia);
            pedido.setProducto("Simulacion de error");
            pedido.setCantidad(1);
        }
        pedido.setTipoError(tipoError);

        eventos.publicar("SIMULACION_CREADA",
                "Pedido de simulacion preparado con error '" + tipoError + "'",
                EventoService.datos(
                        "cliente", pedido.getCliente(),
                        "producto", pedido.getProducto(),
                        "tipoError", tipoError,
                        "origen", base == null ? "nuevo" : "pedido " + base.getPedidoId()
                ));

        return crear(pedido);
    }

    public List<Pedido> crearLote(int cantidad) {
        List<Pedido> creados = new ArrayList<>();
        for (int i = 0; i < cantidad; i++) {
            Pedido pedido = new Pedido();
            pedido.setCliente(CLIENTES_DEMO.get(i % CLIENTES_DEMO.size()));
            pedido.setProducto(PRODUCTOS_DEMO.get(i % PRODUCTOS_DEMO.size()));
            pedido.setCantidad((i % 3) + 1);
            creados.add(crear(pedido));
        }
        eventos.publicar("LOTE_PUBLICADO",
                "Se publicaron " + creados.size()
                        + " pedidos para demostrar competing consumers",
                EventoService.datos("cantidad", creados.size()));
        return creados;
    }

    public List<Pedido> listar() {
        return store.listar();
    }

    public Pedido obtener(Long id) {
        return store.obtener(id);
    }

    private Pedido buscarPorId(String referencia) {
        if (referencia == null || referencia.isBlank()) {
            return null;
        }
        try {
            long id = Long.parseLong(referencia.trim());
            return store.obtener(id);
        } catch (NumberFormatException e) {
            return null;
        }
    }
}
