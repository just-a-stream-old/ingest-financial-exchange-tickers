package finance.modelling.data.ingest.ingestfinancialexchangetickers.api.consumer;

import reactor.core.publisher.Flux;
import reactor.kafka.receiver.ReceiverRecord;

public interface KafkaConsumer<V> {
    Flux<ReceiverRecord<String, V>> receiveMessages(String topic);
}
