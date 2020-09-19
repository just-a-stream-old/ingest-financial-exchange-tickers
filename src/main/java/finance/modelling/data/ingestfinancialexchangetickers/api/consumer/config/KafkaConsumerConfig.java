package finance.modelling.data.ingestfinancialexchangetickers.api.consumer.config;

import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import reactor.kafka.receiver.ReceiverOptions;

import java.util.HashMap;

@Configuration
public class KafkaConsumerConfig {

    private final KafkaProperties kafkaProperties;

    public KafkaConsumerConfig(KafkaProperties kafkaProperties) {
        this.kafkaProperties = kafkaProperties;
    }

    @Bean
    public ReceiverOptions<?, ?> receiverOptions() {
        return ReceiverOptions.create(new HashMap<>(kafkaProperties.buildConsumerProperties()));
    }
}
