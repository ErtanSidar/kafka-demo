package org.essoft.kafkademo.config;

public class Constants {
    public static final String X_AGENT_NAME = "X-AgentName";
    public static final String AGENT_NAME = "tjc-kafka-demo";
    public static final String X_CORRELATION_ID = "X-CorrelationId";
    public static final String X_RETRY_COUNT = "X-RetryCount";
    public static final String ERROR_SUFFIX = ".error";
    public static final String DLQ_SUFFIX = ".dlq";
    public static final int MAX_RETRY_COUNT = 100;
}
