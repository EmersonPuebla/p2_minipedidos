package com.minipedidos.exception;

/**
 * Error que podria resolverse reintentando. El consumidor lo publica en la cola
 * de reintentos hasta agotar la politica configurada.
 */
public class ErrorTemporalException extends RuntimeException {

    public ErrorTemporalException(String message) {
        super(message);
    }

    public ErrorTemporalException(String message, Throwable cause) {
        super(message, cause);
    }
}
