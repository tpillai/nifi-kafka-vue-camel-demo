package com.example.orders.model;

import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

import java.math.BigDecimal;
import java.util.Map;

/** Payload of the public Frankfurter API, forwarded unchanged by NiFi to the fx.rates topic. */
@JsonIgnoreProperties(ignoreUnknown = true)
public record FxRateMessage(String base, String date, Map<String, BigDecimal> rates) {
}
