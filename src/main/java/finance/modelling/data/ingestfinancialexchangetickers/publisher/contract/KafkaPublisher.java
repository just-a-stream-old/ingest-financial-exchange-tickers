package finance.modelling.data.ingestfinancialexchangetickers.publisher.contract;

public interface KafkaPublisher<V> {
    void publishMessage(String topic, V payload);
}
