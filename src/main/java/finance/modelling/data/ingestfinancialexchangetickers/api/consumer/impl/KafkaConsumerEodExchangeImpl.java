package finance.modelling.data.ingestfinancialexchangetickers.api.consumer.impl;

import finance.modelling.data.ingestfinancialexchangetickers.api.consumer.contract.KafkaConsumer;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;

public class KafkaConsumerEodExchangeImpl implements KafkaConsumer {

    private final ReceiverOptions<String, Object> receiverOptions;

    public KafkaConsumerEodExchangeImpl(ReceiverOptions<String, Object> receiverOptions) {
        this.receiverOptions = receiverOptions;
    }

    public Flux<?> receiveMessages() {
        return KafkaReceiver
                .create(receiverOptions)
                .receive();
    }
}
