package finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.config;

import lombok.Getter;
import org.apache.kafka.clients.admin.NewTopic;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.config.TopicBuilder;

@Configuration
@Getter
public class TopicConfig {

    private final Integer numPartitions;
    private final Integer numReplicas;
    private final String traceIdHeaderName;
    private final String fmpQuoteTickerTopic;
    private final String fmpStatementTickerTopic;
    private final String eodExchangeTopic;
    private final String eodTickerTopic;

    public TopicConfig(
            @Value("${kafka.bindings.publisher.partitions}") Integer numPartitions,
            @Value("${kafka.bindings.publisher.replicas}") Integer numReplicas,
            @Value("${spring.kafka.header.traceId}") String traceIdHeaderName,
            @Value("${kafka.bindings.publisher.fmp.fmpQuoteTickers}") String fmpQuoteTickerTopic,
            @Value("${kafka.bindings.publisher.fmp.fmpStatementTickers}") String fmpStatementTickerTopic,
            @Value("${kafka.bindings.publisher.eod.eodExchanges}") String eodExchangeTopic,
            @Value("${kafka.bindings.publisher.eod.eodTickers}") String eodTickerTopic) {
        this.numPartitions = numPartitions;
        this.numReplicas = numReplicas;
        this.traceIdHeaderName = traceIdHeaderName;
        this.fmpQuoteTickerTopic = fmpQuoteTickerTopic;
        this.fmpStatementTickerTopic = fmpStatementTickerTopic;
        this.eodExchangeTopic = eodExchangeTopic;
        this.eodTickerTopic = eodTickerTopic;
    }

    @Bean
    public NewTopic fmpQuoteTickerTopic() {
        return TopicBuilder
                .name(fmpQuoteTickerTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }

    @Bean
    public NewTopic fmpStatementTickerTopic() {
        return TopicBuilder
                .name(fmpStatementTickerTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }

    @Bean
    public NewTopic eodExchangeTopic() {
        return TopicBuilder
                .name(eodExchangeTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }

    @Bean
    public NewTopic eodTickerTopic() {
        return TopicBuilder
                .name(eodTickerTopic)
                .partitions(numPartitions)
                .replicas(numReplicas)
                .compact()
                .build();
    }
}
