package finance.modelling.data.ingestfinancialexchangetickers.api.consumer.contract;

import reactor.core.publisher.Flux;

public interface KafkaConsumer {
    Flux<?> receiveMessages();
}
