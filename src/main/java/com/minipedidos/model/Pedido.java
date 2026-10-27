package com.minipedidos.model;

import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;

/**
 * Pedido que viaja por la cola.
 *
 * Los campos {@code tipoError}, {@code intentos}, {@code ultimoError} y
 * {@code messageId} no forman parte del negocio original del laboratorio:
 * existen para poder demostrar reintentos, DLQ e idempotencia desde el
 * formulario web.
 */
public class Pedido {

    private Long pedidoId;

    @NotBlank(message = "El cliente es obligatorio")
    private String cliente;

    @NotBlank(message = "El producto es obligatorio")
    private String producto;

    @NotNull(message = "La cantidad es obligatoria")
    @Min(value = 1, message = "La cantidad debe ser mayor que cero")
    private Integer cantidad;

    private String estado;

    /** Permite forzar un escenario de error desde el formulario de demo. */
    private String tipoError;

    private Integer intentos;

    private String ultimoError;

    private String messageId;

    private Long fechaCreacion;

    private Long fechaActualizacion;

    public Pedido() {
    }

    public Pedido(Long pedidoId, String cliente, String producto, Integer cantidad) {
        this.pedidoId = pedidoId;
        this.cliente = cliente;
        this.producto = producto;
        this.cantidad = cantidad;
        this.estado = "PENDIENTE";
        this.intentos = 0;
    }

    public Long getPedidoId() {
        return pedidoId;
    }

    public void setPedidoId(Long pedidoId) {
        this.pedidoId = pedidoId;
    }

    public String getCliente() {
        return cliente;
    }

    public void setCliente(String cliente) {
        this.cliente = cliente;
    }

    public String getProducto() {
        return producto;
    }

    public void setProducto(String producto) {
        this.producto = producto;
    }

    public Integer getCantidad() {
        return cantidad;
    }

    public void setCantidad(Integer cantidad) {
        this.cantidad = cantidad;
    }

    public String getEstado() {
        return estado;
    }

    public void setEstado(String estado) {
        this.estado = estado;
    }

    public String getTipoError() {
        return tipoError;
    }

    public void setTipoError(String tipoError) {
        this.tipoError = tipoError;
    }

    public Integer getIntentos() {
        return intentos;
    }

    public void setIntentos(Integer intentos) {
        this.intentos = intentos;
    }

    public String getUltimoError() {
        return ultimoError;
    }

    public void setUltimoError(String ultimoError) {
        this.ultimoError = ultimoError;
    }

    public String getMessageId() {
        return messageId;
    }

    public void setMessageId(String messageId) {
        this.messageId = messageId;
    }

    public Long getFechaCreacion() {
        return fechaCreacion;
    }

    public void setFechaCreacion(Long fechaCreacion) {
        this.fechaCreacion = fechaCreacion;
    }

    public Long getFechaActualizacion() {
        return fechaActualizacion;
    }

    public void setFechaActualizacion(Long fechaActualizacion) {
        this.fechaActualizacion = fechaActualizacion;
    }

    /**
     * Copia defensiva para que los hilos del consumidor y la API no compartan
     * la misma instancia mutable.
     */
    public Pedido copia() {
        Pedido copia = new Pedido();
        copia.pedidoId = pedidoId;
        copia.cliente = cliente;
        copia.producto = producto;
        copia.cantidad = cantidad;
        copia.estado = estado;
        copia.tipoError = tipoError;
        copia.intentos = intentos;
        copia.ultimoError = ultimoError;
        copia.messageId = messageId;
        copia.fechaCreacion = fechaCreacion;
        copia.fechaActualizacion = fechaActualizacion;
        return copia;
    }

    @Override
    public String toString() {
        return "Pedido{" +
                "pedidoId=" + pedidoId +
                ", cliente='" + cliente + '\'' +
                ", producto='" + producto + '\'' +
                ", cantidad=" + cantidad +
                ", estado='" + estado + '\'' +
                ", tipoError='" + tipoError + '\'' +
                ", intentos=" + intentos +
                ", messageId='" + messageId + '\'' +
                '}';
    }
}
