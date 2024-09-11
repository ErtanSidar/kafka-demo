package org.essoft.kafkademo.config;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeader;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.essoft.kafkademo.config.interceptors.KafkaConsumerInterceptor;
import org.essoft.kafkademo.exception.MyCustomException;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.ConcurrentKafkaListenerContainerFactory;
import org.springframework.kafka.core.ConsumerFactory;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.listener.CommonErrorHandler;
import org.springframework.kafka.listener.DeadLetterPublishingRecoverer;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.util.backoff.FixedBackOff;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.essoft.kafkademo.config.Constants.*;

@Configuration
@Slf4j
public class KafkaConsumerConfig {

    public ConsumerFactory<String, Object> consumerFactory() {

        Map<String, Object> config = new HashMap<>();
        config.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.GROUP_ID_CONFIG, "essoft-kafka-demo-app");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        config.put(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, true);

        JsonDeserializer<Object> jsonDeserializer = new JsonDeserializer<>(Object.class);
        jsonDeserializer.addTrustedPackages("*");

        return new DefaultKafkaConsumerFactory<>(config, new StringDeserializer(), jsonDeserializer);
    }


    @Bean
    public ConcurrentKafkaListenerContainerFactory<String, Object> kafkaListenerContainerFactory(CommonErrorHandler commonErrorHandler) {
        ConcurrentKafkaListenerContainerFactory<String, Object> factory = new ConcurrentKafkaListenerContainerFactory<>();

        factory.setConsumerFactory(consumerFactory());
        factory.setRecordInterceptor(new KafkaConsumerInterceptor<>());

        factory.setCommonErrorHandler(commonErrorHandler);

        return factory;
    }

    @Bean
    public CommonErrorHandler commonErrorHandler(KafkaTemplate<String, Object> kafkaTemplate) {
        return new DefaultErrorHandler(
                new DeadLetterPublishingRecoverer(kafkaTemplate, (cr, ex) -> {
                    log.error("Error while processing event: {}", cr, ex);

                    int retryCount = getRetryCountFromHeader(cr);
                    cr.headers().remove(X_RETRY_COUNT);
                    cr.headers().add(new RecordHeader(X_RETRY_COUNT, String.valueOf(retryCount + 1).getBytes()));

                    var topicName = cr.topic().replace(ERROR_SUFFIX, "") + ERROR_SUFFIX;

                    if (retryCount > MAX_RETRY_COUNT || ex.getCause() instanceof MyCustomException) {
                        topicName = cr.topic().replace(ERROR_SUFFIX, "") + DLQ_SUFFIX;
                    }

                    return new TopicPartition(topicName, -1);
                }),
                new FixedBackOff(1, 1)
        );
    }

    private int getRetryCountFromHeader(ConsumerRecord<?, ?> consumerRecord) {
        for (Header header : consumerRecord.headers()) {
            if (Objects.equals(header.key(), X_RETRY_COUNT)) {
                return Integer.parseInt(new String(header.value()));
            }
        }
        return 0;
    }


}
