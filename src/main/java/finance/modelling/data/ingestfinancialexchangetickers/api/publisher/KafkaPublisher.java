package finance.modelling.data.ingestfinancialexchangetickers.api.publisher;

public interface KafkaPublisher<V> {
    void publishMessage(String topic, V payload);
}
