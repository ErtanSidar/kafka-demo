package org.essoft.kafkademo.consumer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.essoft.kafkademo.model.UserValidationResultEvent;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.messaging.handler.annotation.Payload;
import org.springframework.stereotype.Service;

@Service
@Slf4j
@RequiredArgsConstructor
public class UserValidationResultConsumer {

    @KafkaListener(topics = "essoft.kafka-demo-user-validation-result.0", containerFactory = "kafkaListenerContainerFactory")
    public void consume(@Payload UserValidationResultEvent event) {
        handle(event);
    }

    @KafkaListener(topics = "essoft.kafka-demo-user-validation-result.0.error", containerFactory = "kafkaListenerContainerFactory")
    public void consumeError(@Payload UserValidationResultEvent event) {
        handle(event);
    }

    private void handle(UserValidationResultEvent event) {
        log.info("User validated result event received, userId: {}, isValid: {}", event.getUserId(), event.getIsValid());
    }

}
