package com.example.orderservice.kafka;

import com.example.orderservice.model.OutboxEvent;
import com.example.orderservice.repository.OutboxEventRepository;
import com.fasterxml.jackson.databind.ObjectMapper;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;

@Service
@RequiredArgsConstructor
@Slf4j
public class OrderEventPublisher {

    public static final String ORDER_TOPIC = "order-events";
    private static final int MAX_RETRIES   = 3;

    private final KafkaTemplate<String, String> kafkaTemplate;
    private final OutboxEventRepository outboxRepo;
    private final ObjectMapper objectMapper;

    /**
     * Outbox Relay — runs every 5 seconds.
     * Picks up PENDING outbox events and publishes them to Kafka.
     * On success → marks PUBLISHED. On failure → increments retryCount.
     * After MAX_RETRIES → marks FAILED (alerts/dead-letter handling can follow).
     */
    @Scheduled(fixedDelay = 5000)
    @Transactional
    public void relayOutboxEvents() {
        List<OutboxEvent> pending = outboxRepo
                .findByStatusAndRetryCountLessThan(OutboxEvent.EventStatus.PENDING, MAX_RETRIES);

        for (OutboxEvent event : pending) {
            try {
                kafkaTemplate.send(ORDER_TOPIC, event.getAggregateId(), event.getPayload())
                        .get(); // block to confirm send before marking published

                event.setStatus(OutboxEvent.EventStatus.PUBLISHED);
                event.setProcessedAt(LocalDateTime.now());
                log.info("Published outbox event id={} type={}", event.getId(), event.getEventType());

            } catch (Exception e) {
                event.setRetryCount(event.getRetryCount() + 1);
                if (event.getRetryCount() >= MAX_RETRIES) {
                    event.setStatus(OutboxEvent.EventStatus.FAILED);
                    log.error("Outbox event id={} permanently failed after {} retries",
                            event.getId(), MAX_RETRIES);
                } else {
                    log.warn("Outbox event id={} failed, retry {}/{}",
                            event.getId(), event.getRetryCount(), MAX_RETRIES);
                }
            }
            outboxRepo.save(event);
        }
    }
}
