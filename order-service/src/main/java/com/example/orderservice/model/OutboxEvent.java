package com.example.orderservice.model;

import jakarta.persistence.*;
import lombok.*;
import java.time.LocalDateTime;

/**
 * Outbox Pattern — events are first written to this table inside the same DB
 * transaction as the order. A scheduler then publishes them to Kafka.
 * This guarantees at-least-once delivery even if Kafka is temporarily down.
 */
@Entity
@Table(name = "outbox_events")
@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OutboxEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private String aggregateType;   // e.g. "Order"

    @Column(nullable = false)
    private String aggregateId;     // order number

    @Column(nullable = false)
    private String eventType;       // e.g. "ORDER_CREATED"

    @Column(nullable = false, columnDefinition = "TEXT")
    private String payload;         // JSON string

    @Enumerated(EnumType.STRING)
    @Column(nullable = false)
    private EventStatus status;

    private int retryCount;

    @Column(nullable = false, updatable = false)
    private LocalDateTime createdAt;

    private LocalDateTime processedAt;

    @PrePersist
    protected void onCreate() {
        createdAt = LocalDateTime.now();
        if (status == null) status = EventStatus.PENDING;
    }

    public enum EventStatus {
        PENDING, PUBLISHED, FAILED
    }
}
