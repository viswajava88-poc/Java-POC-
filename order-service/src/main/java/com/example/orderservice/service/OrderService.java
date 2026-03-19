package com.example.orderservice.service;

import com.example.orderservice.dto.OrderDto.*;
import com.example.orderservice.exception.DuplicateOrderException;
import com.example.orderservice.exception.OrderNotFoundException;
import com.example.orderservice.model.Order;
import com.example.orderservice.model.OrderItem;
import com.example.orderservice.model.OutboxEvent;
import com.example.orderservice.repository.OrderRepository;
import com.example.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.math.BigDecimal;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderService {

    private final OrderRepository     orderRepo;
    private final OutboxEventRepository outboxRepo;
    private final ObjectMapper         objectMapper;
    private final MeterRegistry        meterRegistry;

    /**
     * POST /orders — Create a new order.
     *
     * Idempotency: if the request carries an idempotencyKey that already exists
     * in the DB, the existing order is returned without re-processing.
     *
     * Outbox Pattern: the Kafka event is written to `outbox_events` inside the
     * same transaction as the order, guaranteeing no event is lost even if
     * Kafka is temporarily unavailable.
     */
    @Transactional
    public OrderResponse createOrder(CreateOrderRequest req) {
        log.info("Creating order for customer={} idempotencyKey={}",
                req.getCustomerEmail(), req.getIdempotencyKey());

        // ── Idempotency check ─────────────────────────────────────────
        if (req.getIdempotencyKey() != null) {
            Optional<Order> existing = orderRepo.findByIdempotencyKey(req.getIdempotencyKey());
            if (existing.isPresent()) {
                log.warn("Duplicate order detected for idempotencyKey={}", req.getIdempotencyKey());
                meterRegistry.counter("orders.duplicate").increment();
                return mapToResponse(existing.get());
            }
        }

        // ── Build items ───────────────────────────────────────────────
        List<OrderItem> items = req.getItems().stream().map(i -> {
            BigDecimal sub = i.getUnitPrice().multiply(BigDecimal.valueOf(i.getQuantity()));
            return OrderItem.builder()
                    .productId(i.getProductId())
                    .productName(i.getProductName())
                    .quantity(i.getQuantity())
                    .unitPrice(i.getUnitPrice())
                    .subTotal(sub)
                    .build();
        }).collect(Collectors.toList());

        BigDecimal total = items.stream()
                .map(OrderItem::getSubTotal)
                .reduce(BigDecimal.ZERO, BigDecimal::add);

        // ── Persist order ─────────────────────────────────────────────
        Order order = Order.builder()
                .customerName(req.getCustomerName())
                .customerEmail(req.getCustomerEmail())
                .status(Order.OrderStatus.PENDING)
                .totalAmount(total)
                .idempotencyKey(req.getIdempotencyKey())
                .items(items)
                .build();

        items.forEach(i -> i.setOrder(order));
        Order saved = orderRepo.save(order);

        // ── Write outbox event (same transaction) ─────────────────────
        writeOutboxEvent(saved, "ORDER_CREATED");

        meterRegistry.counter("orders.created").increment();
        log.info("Order created orderNumber={}", saved.getOrderNumber());
        return mapToResponse(saved);
    }

    /**
     * GET /orders/{id} — Retrieve a single order by its database ID.
     */
    @Transactional(readOnly = true)
    public OrderResponse getOrderById(Long id) {
        log.info("Fetching order id={}", id);
        Order order = orderRepo.findById(id)
                .orElseThrow(() -> new OrderNotFoundException("Order not found: " + id));
        return mapToResponse(order);
    }

    // ─── Helpers ──────────────────────────────────────────────────────

    private void writeOutboxEvent(Order order, String eventType) {
        OrderEventPayload payload = OrderEventPayload.builder()
                .orderNumber(order.getOrderNumber())
                .customerName(order.getCustomerName())
                .customerEmail(order.getCustomerEmail())
                .status(order.getStatus().name())
                .totalAmount(order.getTotalAmount())
                .eventType(eventType)
                .eventTime(LocalDateTime.now())
                .build();

        String json;
        try {
            json = objectMapper.writeValueAsString(payload);
        } catch (JsonProcessingException e) {
            throw new RuntimeException("Failed to serialize order event", e);
        }

        outboxRepo.save(OutboxEvent.builder()
                .aggregateType("Order")
                .aggregateId(order.getOrderNumber())
                .eventType(eventType)
                .payload(json)
                .status(OutboxEvent.EventStatus.PENDING)
                .retryCount(0)
                .build());
    }

    private OrderResponse mapToResponse(Order order) {
        List<OrderItemResponse> itemResponses = order.getItems().stream()
                .map(i -> OrderItemResponse.builder()
                        .id(i.getId())
                        .productId(i.getProductId())
                        .productName(i.getProductName())
                        .quantity(i.getQuantity())
                        .unitPrice(i.getUnitPrice())
                        .subTotal(i.getSubTotal())
                        .build())
                .collect(Collectors.toList());

        return OrderResponse.builder()
                .id(order.getId())
                .orderNumber(order.getOrderNumber())
                .customerName(order.getCustomerName())
                .customerEmail(order.getCustomerEmail())
                .status(order.getStatus())
                .totalAmount(order.getTotalAmount())
                .items(itemResponses)
                .createdAt(order.getCreatedAt())
                .updatedAt(order.getUpdatedAt())
                .build();
    }
}
