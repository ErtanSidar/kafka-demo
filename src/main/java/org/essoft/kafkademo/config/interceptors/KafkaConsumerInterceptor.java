package org.essoft.kafkademo.config.interceptors;

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.MDC;
import org.springframework.kafka.listener.RecordInterceptor;

import java.util.Objects;
import java.util.UUID;

import static io.micrometer.common.util.StringUtils.isNotEmpty;
import static org.essoft.kafkademo.config.Constants.*;

@Slf4j
public class KafkaConsumerInterceptor<T> implements RecordInterceptor<String, T> {
    @Override
    public ConsumerRecord<String, T> intercept(ConsumerRecord<String, T> record, Consumer<String, T> consumer) {
        setCorrelationId(record);
        setAgentName(record);
        return record;
    }

    private void setCorrelationId(ConsumerRecord<String, T> consumerRecord) {
        var correlationId = getHeaderStringValueByKey(consumerRecord, X_CORRELATION_ID);
        if (isNotEmpty(correlationId)) {
            MDC.put(X_CORRELATION_ID, correlationId);
            return;
        }
        MDC.put(X_CORRELATION_ID, UUID.randomUUID().toString());
    }

    private void setAgentName(ConsumerRecord<String, T> consumerRecord) {
        var agentName = getHeaderStringValueByKey(consumerRecord, X_AGENT_NAME);
        if (isNotEmpty(agentName)) {
            MDC.put(X_AGENT_NAME, agentName);
            return;
        }
        MDC.put(X_AGENT_NAME, AGENT_NAME);
    }

    private String getHeaderStringValueByKey(ConsumerRecord<String, T> consumerRecord, String headerKey) {
        for (Header header : consumerRecord.headers()) {
            if (Objects.equals(header.key(), headerKey)) {
                return new String(header.value());
            }
        }
        return "";
    }

}
