package com.minipedidos.service;

import com.minipedidos.exception.ErrorPermanenteException;
import com.minipedidos.exception.ErrorTemporalException;
import com.minipedidos.model.Pedido;
import org.springframework.stereotype.Component;

/**
 * Traduce el campo de demo {@code tipoError} (o nombres especiales de cliente)
 * a un error permanente o temporal. En una aplicacion real esta decision la
 * tomaria el servicio de negocio segun el tipo de excepcion.
 */
@Component
public class SimuladorErrores {

    public static final String RECHAZADO = "rechazado";
    public static final String REQUEUE = "requeue";
    public static final String TIMEOUT = "timeout";
    public static final String FALLO = "fallo";
    public static final String TEMPORAL = "temporal";
    public static final String ERROR = "error";
    public static final String DLQ_RECUPERABLE = "dlq-recuperable";

    /**
     * @param pedido    pedido a evaluar
     * @param reintento reintentos ya realizados (0 en la primera entrega)
     */
    public void evaluar(Pedido pedido, int reintento) {
        evaluar(pedido, reintento, false);
    }

    /**
     * @param reprocesado indica si el mensaje viene de una accion "reprocesar"
     *                    de la DLQ. Permite demostrar una recuperacion real
     *                    despues de inspeccionar y corregir la causa.
     */
    public void evaluar(Pedido pedido, int reintento, boolean reprocesado) {
        String tipo = normalizar(pedido.getTipoError());

        // El cliente reservado "error" tiene prioridad absoluta: aunque se
        // haya elegido otra accion en el formulario, el resultado es NACK.
        if (esClienteError(pedido.getCliente())) {
            tipo = ERROR;
        } else if (tipo == null) {
            tipo = porCliente(pedido.getCliente());
        }

        if (tipo == null) {
            return;
        }

        switch (tipo) {
            case ERROR, RECHAZADO, FALLO ->
                    throw new ErrorPermanenteException(
                            "Error permanente simulado (" + tipo + ")"
                    );

            case DLQ_RECUPERABLE -> {
                // Primera entrega: error permanente -> DLQ.
                // Al reprocesar desde la DLQ, el mensaje se procesa bien.
                if (!reprocesado) {
                    throw new ErrorPermanenteException(
                            "Fallo permanente que se corrige al reprocesar desde la DLQ"
                    );
                }
            }

            case TEMPORAL, REQUEUE -> {
                // El primer intento falla; el reintento se procesa bien.
                if (reintento == 0 && !reprocesado) {
                    throw new ErrorTemporalException(
                            "Servicio temporalmente no disponible; se reintentara"
                    );
                }
            }

            case TIMEOUT ->
                    // Falla siempre: agota los reintentos y termina en la DLQ.
                    throw new ErrorTemporalException(
                            "Timeout simulado del servicio de inventario"
                    );

            default -> {
                // Sin error simulado.
            }
        }
    }

    public boolean esTimeout(Pedido pedido) {
        return TIMEOUT.equals(normalizar(pedido.getTipoError()));
    }

    public static String normalizar(String valor) {
        if (valor == null || valor.isBlank()) {
            return null;
        }
        return valor.trim().toLowerCase();
    }

    private boolean esClienteError(String cliente) {
        return cliente != null && ERROR.equalsIgnoreCase(cliente.trim());
    }

    /**
     * Nombres de cliente reservados para la demo. La comparacion es
     * case-insensitive y tolera espacios: "ERROR", "error" y "ErRoR"
     * producen un NACK permanente.
     */
    private String porCliente(String cliente) {
        if (cliente == null) {
            return null;
        }
        String normalizado = cliente.trim();
        if (ERROR.equalsIgnoreCase(normalizado)) {
            return ERROR;
        }
        if (TEMPORAL.equalsIgnoreCase(normalizado)) {
            return TEMPORAL;
        }
        return null;
    }
}
