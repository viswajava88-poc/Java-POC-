package com.example.notificationservice.websocket;

import com.example.notificationservice.model.OrderEvent;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.messaging.simp.SimpMessagingTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class NotificationBroadcaster {

    private final SimpMessagingTemplate messagingTemplate;

    /**
     * Broadcasts an order event to ALL clients subscribed to /topic/orders.
     * SimpMessagingTemplate handles thread safety and session management.
     */
    public void broadcast(OrderEvent event) {
        log.info("Broadcasting event orderNumber={} status={} to /topic/orders",
                event.getOrderNumber(), event.getStatus());
        messagingTemplate.convertAndSend("/topic/orders", event);
    }

    /**
     * Broadcasts to a customer-specific channel: /topic/orders/{email}
     * Allows the frontend to subscribe only to its own order updates.
     */
    public void broadcastToCustomer(OrderEvent event) {
        String destination = "/topic/orders/" + event.getCustomerEmail().replace("@", "_at_");
        log.info("Broadcasting to customer channel {}", destination);
        messagingTemplate.convertAndSend(destination, event);
        // Also broadcast to the general channel
        broadcast(event);
    }
}
