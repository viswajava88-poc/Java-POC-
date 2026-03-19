package com.example.notificationservice.kafka;

import com.example.notificationservice.model.OrderEvent;
import com.example.notificationservice.websocket.NotificationBroadcaster;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.micrometer.core.instrument.MeterRegistry;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.kafka.support.KafkaHeaders;
import org.springframework.messaging.handler.annotation.Header;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderEventConsumer {

    private final NotificationBroadcaster broadcaster;
    private final ObjectMapper objectMapper;
    private final MeterRegistry meterRegistry;

    /**
     * Consumes messages from the "order-events" Kafka topic.
     *
     * - groupId "notification-service" ensures this service gets its own offset.
     * - Manual acknowledgment (AckMode.MANUAL) means the offset is only committed
     *   AFTER the message has been successfully broadcast — preventing data loss
     *   if the service crashes mid-processing.
     * - On deserialization failure the message is logged and skipped (dead-letter
     *   handling can be added here for production).
     */
    @KafkaListener(
            topics = "order-events",
            groupId = "notification-service",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(
            @Payload String message,
            @Header(KafkaHeaders.RECEIVED_PARTITION) int partition,
            @Header(KafkaHeaders.OFFSET) long offset,
            Acknowledgment acknowledgment) {

        log.info("Received Kafka message partition={} offset={}", partition, offset);

        try {
            OrderEvent event = objectMapper.readValue(message, OrderEvent.class);
            log.info("Processing event type={} orderNumber={}", event.getEventType(), event.getOrderNumber());

            broadcaster.broadcast(event);

            meterRegistry.counter("notifications.sent",
                    "eventType", event.getEventType()).increment();

            acknowledgment.acknowledge(); // commit offset only after successful processing

        } catch (Exception e) {
            log.error("Failed to process Kafka message offset={}: {}", offset, e.getMessage(), e);
            meterRegistry.counter("notifications.failed").increment();
            // Acknowledge anyway to avoid poison-pill infinite loop.
            // In production: route to a Dead Letter Topic instead.
            acknowledgment.acknowledge();
        }
    }
}
