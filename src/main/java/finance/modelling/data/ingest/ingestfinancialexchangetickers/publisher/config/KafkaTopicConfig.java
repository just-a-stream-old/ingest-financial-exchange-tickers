package finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.config;

import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
public class KafkaTopicConfig {

    private final Integer numPartitions;
    private final Integer numReplicas;
    private final String outputFmpTickerTopic;
    private final String outputEodExchangeTopic;
    private final String outputEodTickerTopic;

    public KafkaTopicConfig(
            @Value("${kafka.bindings.publisher.partitions}") Integer numPartitions,
            @Value("${kafka.bindings.publisher.replicas}") Integer numReplicas,
            @Value("${kafka.bindings.publisher.fmp.fmpTickers}") String outputFmpTickerTopic,
            @Value("${kafka.bindings.publisher.eod.eodExchanges}") String outputEodExchangeTopic,
            @Value("${kafka.bindings.publisher.eod.eodTickers}") String outputEodTickerTopic) {
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
        this.outputFmpTickerTopic = outputFmpTickerTopic;
        this.outputEodExchangeTopic = outputEodExchangeTopic;
        this.outputEodTickerTopic = outputEodTickerTopic;
    }

    @Bean
    public NewTopic outputFmpTickerTopic() {
        return TopicBuilder
                .name(outputFmpTickerTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }

    @Bean
    public NewTopic outputEodExchangeTopic() {
        return TopicBuilder
                .name(outputEodExchangeTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }

    @Bean
    public NewTopic outputEodTickerTopic() {
        return TopicBuilder
                .name(outputEodTickerTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }
}
