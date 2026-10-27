package com.minipedidos.exception;

/**
 * Error que no se recupera repitiendo la operacion. El consumidor lo rechaza
 * inmediatamente y RabbitMQ lo envia a la DLX/DLQ.
 */
public class ErrorPermanenteException extends RuntimeException {

    public ErrorPermanenteException(String message) {
        super(message);
    }

    public ErrorPermanenteException(String message, Throwable cause) {
        super(message, cause);
    }
}
