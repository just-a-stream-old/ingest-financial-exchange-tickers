package finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.contract;

public interface KafkaPublisher<V> {
    void publishMessage(String topic, V payload);
}
