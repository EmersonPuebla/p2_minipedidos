package com.minipedidos.controller;

import com.minipedidos.service.MetricasService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.Map;

@RestController
@RequestMapping("/api")
public class MetricasController {

    private final MetricasService metricasService;

    public MetricasController(MetricasService metricasService) {
        this.metricasService = metricasService;
    }

    @GetMapping("/metricas")
    public Map<String, Object> metricas() {
        return metricasService.obtener();
    }

    @GetMapping("/salud")
    public Map<String, Object> salud() {
        return metricasService.salud();
    }
}
