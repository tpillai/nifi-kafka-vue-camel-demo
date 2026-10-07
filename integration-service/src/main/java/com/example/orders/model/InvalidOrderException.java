package com.example.orders.model;

/** The message parsed but breaks a business rule. Retrying will not help, so it goes straight to the DLT. */
public class InvalidOrderException extends RuntimeException {
    public InvalidOrderException(String message) {
        super(message);
    }
}
