package com.example.orders.routes;

import com.example.orders.model.InvalidOrderException;
import com.example.orders.model.Order;
import com.example.orders.model.UnknownCustomerException;
import com.example.orders.persistence.ReferenceDataRepository;
import com.example.orders.persistence.ReferenceDataRepository.Customer;
import com.example.orders.persistence.ReferenceDataRepository.FxRate;
import org.springframework.stereotype.Component;

import java.math.RoundingMode;
import java.time.Instant;
import java.util.Set;
import java.util.UUID;

/** Plain Java steps called from the Camel route, kept here so they are unit-testable without Camel. */
@Component("orderProcessor")
public class OrderProcessor {

    private static final Set<String> PRIORITIES = Set.of("HIGH", "NORMAL");

    private final ReferenceDataRepository referenceData;

    public OrderProcessor(ReferenceDataRepository referenceData) {
        this.referenceData = referenceData;
    }

    /** Business validation. Throws {@link InvalidOrderException}, which the route sends straight to the DLT. */
    public Order validate(Order o) {
        if (o.getOrderId() == null) {
            throw new InvalidOrderException("orderId is required");
        }
        try {
            UUID.fromString(o.getOrderId());
        } catch (IllegalArgumentException e) {
            throw new InvalidOrderException("orderId must be a UUID: " + o.getOrderId());
        }
        if (isBlank(o.getCustomerId())) {
            throw new InvalidOrderException("customerId is required");
        }
        if (isBlank(o.getProduct())) {
            throw new InvalidOrderException("product is required");
        }
        if (o.getQuantity() == null || o.getQuantity() <= 0) {
            throw new InvalidOrderException("quantity must be greater than 0");
        }
        if (o.getAmount() == null || o.getAmount().signum() <= 0) {
            throw new InvalidOrderException("amount must be greater than 0");
        }
        o.setCurrency(isBlank(o.getCurrency()) ? "USD" : o.getCurrency().trim().toUpperCase());
        o.setPriority(isBlank(o.getPriority()) ? "NORMAL" : o.getPriority().trim().toUpperCase());
        if (!PRIORITIES.contains(o.getPriority())) {
            throw new InvalidOrderException("priority must be HIGH or NORMAL");
        }
        if (isBlank(o.getReceivedAt())) {
            o.setReceivedAt(Instant.now().toString());
        }
        if (isBlank(o.getSource())) {
            o.setSource("unknown");
        }
        return o;
    }

    /** Content enricher: customer master data plus currency conversion to USD. */
    public Order enrich(Order o) {
        Customer customer = referenceData.findCustomer(o.getCustomerId())
                .orElseThrow(() -> new UnknownCustomerException(o.getCustomerId()));
        o.setCustomerName(customer.name());
        o.setCustomerTier(customer.tier());
        o.setCreditLimitUsd(customer.creditLimitUsd());

        FxRate rate = referenceData.findRate(o.getCurrency())
                .orElseThrow(() -> new InvalidOrderException("Unsupported currency " + o.getCurrency()));
        o.setFxRate(rate.ratePerUsd());
        o.setAmountUsd(o.getAmount().divide(rate.ratePerUsd(), 2, RoundingMode.HALF_UP));
        return o;
    }

    public boolean exceedsCreditLimit(Order o) {
        return o.getAmountUsd().compareTo(o.getCreditLimitUsd()) > 0;
    }

    public boolean isExpress(Order o) {
        return "HIGH".equals(o.getPriority()) || "GOLD".equals(o.getCustomerTier());
    }

    public void rejectForCredit(Order o) {
        o.reject("Amount " + o.getAmountUsd() + " USD exceeds credit limit "
                + o.getCreditLimitUsd().setScale(2, RoundingMode.HALF_UP) + " USD");
    }

    private static boolean isBlank(String s) {
        return s == null || s.isBlank();
    }
}
