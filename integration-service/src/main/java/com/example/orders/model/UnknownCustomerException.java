package com.example.orders.model;

/** Enrichment found no customer for the order. Not retryable, so it goes straight to the DLT. */
public class UnknownCustomerException extends RuntimeException {
    public UnknownCustomerException(String customerId) {
        super("Unknown customer " + customerId);
    }
}
