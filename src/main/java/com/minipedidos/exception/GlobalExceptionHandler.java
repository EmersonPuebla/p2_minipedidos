package com.minipedidos.exception;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.AmqpException;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;
import org.springframework.web.servlet.resource.NoResourceFoundException;

import java.util.LinkedHashMap;
import java.util.Map;
import java.util.NoSuchElementException;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> validacion(
            MethodArgumentNotValidException ex) {

        Map<String, String> campos = new LinkedHashMap<>();
        for (FieldError error : ex.getBindingResult().getFieldErrors()) {
            campos.put(error.getField(), error.getDefaultMessage());
        }

        Map<String, Object> cuerpo = cuerpo(
                HttpStatus.BAD_REQUEST,
                "Datos invalidos",
                "Revisa los campos del formulario"
        );
        cuerpo.put("campos", campos);
        return ResponseEntity.badRequest().body(cuerpo);
    }

    @ExceptionHandler(NoSuchElementException.class)
    public ResponseEntity<Map<String, Object>> noEncontrado(NoSuchElementException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(cuerpo(HttpStatus.NOT_FOUND, "No encontrado", ex.getMessage()));
    }

    @ExceptionHandler(NoResourceFoundException.class)
    public ResponseEntity<Map<String, Object>> noRecurso(NoResourceFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND)
                .body(cuerpo(
                        HttpStatus.NOT_FOUND,
                        "No encontrado",
                        "El recurso solicitado no existe"
                ));
    }

    @ExceptionHandler(IllegalArgumentException.class)
    public ResponseEntity<Map<String, Object>> argumento(IllegalArgumentException ex) {
        return ResponseEntity.badRequest()
                .body(cuerpo(HttpStatus.BAD_REQUEST, "Solicitud invalida", ex.getMessage()));
    }

    @ExceptionHandler(IllegalStateException.class)
    public ResponseEntity<Map<String, Object>> estado(IllegalStateException ex) {
        return ResponseEntity.status(HttpStatus.CONFLICT)
                .body(cuerpo(HttpStatus.CONFLICT, "Conflicto", ex.getMessage()));
    }

    @ExceptionHandler(AmqpException.class)
    public ResponseEntity<Map<String, Object>> amqp(AmqpException ex) {
        String detalle = mensajeCompleto(ex);
        log.warn("Error RabbitMQ: {}", detalle);

        if (detalle.contains("PRECONDITION_FAILED")
                || detalle.contains("inequivalent")) {
            return ResponseEntity.status(HttpStatus.CONFLICT)
                    .body(cuerpo(
                            HttpStatus.CONFLICT,
                            "Conflicto de topologia",
                            "La cola ya existe con otra configuracion. "
                                    + "Marca 'Recrear cola' en el formulario o usa otro nombre."
                    ));
        }

        Map<String, Object> cuerpo = cuerpo(
                HttpStatus.SERVICE_UNAVAILABLE,
                "RabbitMQ no disponible",
                detalle
        );
        cuerpo.put("sugerencia",
                "Verifica que RabbitMQ este iniciado: docker compose up -d");
        return ResponseEntity.status(HttpStatus.SERVICE_UNAVAILABLE).body(cuerpo);
    }

    private String mensajeCompleto(Throwable error) {
        StringBuilder texto = new StringBuilder();
        Throwable actual = error;
        int profundidad = 0;
        while (actual != null && profundidad++ < 12) {
            if (actual.getMessage() != null) {
                if (!texto.isEmpty()) {
                    texto.append(" <- ");
                }
                texto.append(actual.getMessage());
            }
            actual = actual.getCause();
        }
        return texto.isEmpty() ? "Sin detalle" : texto.toString();
    }

    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> general(Exception ex) {
        log.error("Error no controlado", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR)
                .body(cuerpo(
                        HttpStatus.INTERNAL_SERVER_ERROR,
                        "Error interno",
                        ex.getMessage() == null ? "Sin detalle" : ex.getMessage()
                ));
    }

    private Map<String, Object> cuerpo(
            HttpStatus status,
            String error,
            String mensaje) {

        Map<String, Object> cuerpo = new LinkedHashMap<>();
        cuerpo.put("timestamp", System.currentTimeMillis());
        cuerpo.put("status", status.value());
        cuerpo.put("error", error);
        cuerpo.put("mensaje", mensaje);
        return cuerpo;
    }
}
