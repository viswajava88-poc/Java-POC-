package com.example.orderservice.dto;

import com.example.orderservice.model.Order;
import jakarta.validation.Valid;
import jakarta.validation.constraints.*;
import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;

public class OrderDto {

    // ─── Inbound ───────────────────────────────────────────────────

    @Data @NoArgsConstructor @AllArgsConstructor @Builder
    public static class CreateOrderRequest {

        @NotBlank(message = "Customer name is required")
        private String customerName;

        @NotBlank(message = "Customer email is required")
        @Email(message = "Invalid email")
        private String customerEmail;

        @NotEmpty(message = "At least one item is required")
        @Valid
        private List<OrderItemRequest> items;

        /**
         * Optional idempotency key from the client.
         * If the same key is submitted again, the original order is returned
         * instead of creating a duplicate.
         */
        private String idempotencyKey;
    }

    @Data @NoArgsConstructor @AllArgsConstructor @Builder
    public static class OrderItemRequest {

        @NotBlank private String productId;
        @NotBlank private String productName;

        @NotNull @Min(1)
        private Integer quantity;

        @NotNull @DecimalMin("0.01")
        private BigDecimal unitPrice;
    }

    // ─── Outbound ──────────────────────────────────────────────────

    @Data @NoArgsConstructor @AllArgsConstructor @Builder
    public static class OrderResponse {
        private Long id;
        private String orderNumber;
        private String customerName;
        private String customerEmail;
        private Order.OrderStatus status;
        private BigDecimal totalAmount;
        private List<OrderItemResponse> items;
        private LocalDateTime createdAt;
        private LocalDateTime updatedAt;
    }

    @Data @NoArgsConstructor @AllArgsConstructor @Builder
    public static class OrderItemResponse {
        private Long id;
        private String productId;
        private String productName;
        private Integer quantity;
        private BigDecimal unitPrice;
        private BigDecimal subTotal;
    }

    @Data @NoArgsConstructor @AllArgsConstructor @Builder
    public static class ApiResponse<T> {
        private boolean success;
        private String message;
        private T data;

        public static <T> ApiResponse<T> success(String msg, T data) {
            return ApiResponse.<T>builder().success(true).message(msg).data(data).build();
        }

        public static <T> ApiResponse<T> error(String msg) {
            return ApiResponse.<T>builder().success(false).message(msg).build();
        }
    }

    // ─── Kafka Event Payload ────────────────────────────────────────

    @Data @NoArgsConstructor @AllArgsConstructor @Builder
    public static class OrderEventPayload {
        private String orderNumber;
        private String customerName;
        private String customerEmail;
        private String status;
        private BigDecimal totalAmount;
        private String eventType;
        private LocalDateTime eventTime;
    }
}
