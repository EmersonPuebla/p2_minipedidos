package com.minipedidos.service;

import com.minipedidos.model.PoliticaReintentos;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

class PoliticaReintentosServiceTest {

    @Test
    void usaLaPoliticaInicial() {
        PoliticaReintentosService service = new PoliticaReintentosService(3, 5);

        assertEquals(3, service.getPolitica().maxReintentos());
        assertEquals(5, service.getPolitica().intervaloSegundos());
        assertEquals(5_000L, service.getPolitica().intervaloMs());
    }

    @Test
    void actualizaLaPolitica() {
        PoliticaReintentosService service = new PoliticaReintentosService(3, 5);

        PoliticaReintentos nueva = service.actualizar(1, 2);

        assertEquals(1, nueva.maxReintentos());
        assertEquals(2, nueva.intervaloSegundos());
        assertEquals(1, service.getPolitica().maxReintentos());
    }

    @Test
    void rechazaValoresFueraDeRango() {
        PoliticaReintentosService service = new PoliticaReintentosService(3, 5);

        assertThrows(IllegalArgumentException.class,
                () -> service.actualizar(11, 5));
        assertThrows(IllegalArgumentException.class,
                () -> service.actualizar(-1, 5));
        assertThrows(IllegalArgumentException.class,
                () -> service.actualizar(2, 0));
        assertThrows(IllegalArgumentException.class,
                () -> service.actualizar(2, 301));
    }

    @Test
    void limitaLosValoresIniciales() {
        PoliticaReintentosService service = new PoliticaReintentosService(99, 0);

        assertEquals(10, service.getPolitica().maxReintentos());
        assertEquals(1, service.getPolitica().intervaloSegundos());
    }
}
