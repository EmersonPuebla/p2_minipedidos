package com.minipedidos.service;

import com.minipedidos.exception.ErrorPermanenteException;
import com.minipedidos.model.Pedido;
import org.springframework.stereotype.Component;

/**
 * Validacion que se ejecuta en el consumidor. Aunque la API ya valida con
 * Jakarta Validation, un mensaje puede llegar desde otro productor o desde la
 * DLQ; por eso el consumidor vuelve a validar antes de procesar.
 */
@Component
public class PedidoValidator {

    public void validar(Pedido pedido) {
        if (pedido == null) {
            throw new ErrorPermanenteException("El mensaje no contiene un pedido");
        }
        if (vacio(pedido.getCliente())) {
            throw new ErrorPermanenteException("El cliente es obligatorio");
        }
        if (vacio(pedido.getProducto())) {
            throw new ErrorPermanenteException("El producto es obligatorio");
        }
        if (pedido.getCantidad() == null || pedido.getCantidad() < 1) {
            throw new ErrorPermanenteException("La cantidad debe ser mayor que cero");
        }
        if (pedido.getCantidad() > 10_000) {
            throw new ErrorPermanenteException("La cantidad supera el maximo permitido");
        }
    }

    public static boolean vacio(String valor) {
        return valor == null || valor.isBlank();
    }
}
