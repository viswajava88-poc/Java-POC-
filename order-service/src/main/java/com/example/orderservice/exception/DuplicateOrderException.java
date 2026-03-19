package com.example.orderservice.exception;

public class DuplicateOrderException extends RuntimeException {
    public DuplicateOrderException(String message) { super(message); }
}
