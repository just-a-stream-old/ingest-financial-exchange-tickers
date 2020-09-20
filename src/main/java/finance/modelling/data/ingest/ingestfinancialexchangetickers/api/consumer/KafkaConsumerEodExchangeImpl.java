package finance.modelling.data.ingest.ingestfinancialexchangetickers.api.consumer;

import finance.modelling.fmcommons.data.schema.eod.dto.EodExchangeDTO;
import org.springframework.stereotype.Component;
import reactor.core.publisher.Flux;
import reactor.kafka.receiver.KafkaReceiver;
import reactor.kafka.receiver.ReceiverOptions;
import reactor.kafka.receiver.ReceiverRecord;

import java.util.Collections;

@Component
public class KafkaConsumerEodExchangeImpl implements KafkaConsumer<EodExchangeDTO> {

    private final ReceiverOptions<String, EodExchangeDTO> receiverOptions;

    public KafkaConsumerEodExchangeImpl(ReceiverOptions<String, EodExchangeDTO> receiverOptions) {
        this.receiverOptions = receiverOptions;
    }

    public Flux<ReceiverRecord<String, EodExchangeDTO>> receiveMessages(String topic) {
        return KafkaReceiver
                .create(receiverOptions.subscription(Collections.singleton(topic)))
                .receive();
    }
}
