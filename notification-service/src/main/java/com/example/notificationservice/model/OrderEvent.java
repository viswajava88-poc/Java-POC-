package com.example.notificationservice.model;

import lombok.*;
import java.math.BigDecimal;
import java.time.LocalDateTime;

@Data
@NoArgsConstructor
@AllArgsConstructor
@Builder
public class OrderEvent {
    private String orderNumber;
    private String customerName;
    private String customerEmail;
    private String status;
    private BigDecimal totalAmount;
    private String eventType;
    private LocalDateTime eventTime;
}
