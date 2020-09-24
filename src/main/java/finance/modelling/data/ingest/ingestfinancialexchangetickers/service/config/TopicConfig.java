package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

@Configuration
@Getter
public class TopicConfig {

    private final String traceIdHeaderName;
    private final String eodExchangeTopic;
    private final String eodTickerTopic;
    private final String fmpTickerTopic;

    public TopicConfig(
            @Value("${spring.kafka.trace.header}") String traceIdHeaderName,
            @Value("${kafka.bindings.publisher.eod.eodExchanges}") String eodExchangeTopic,
            @Value("${kafka.bindings.publisher.eod.eodTickers}") String eodTickerTopic,
            @Value("${kafka.bindings.publisher.fmp.fmpTickers}") String fmpTickerTopic) {
        this.traceIdHeaderName = traceIdHeaderName;
        this.eodExchangeTopic = eodExchangeTopic;
        this.eodTickerTopic = eodTickerTopic;
        this.fmpTickerTopic = fmpTickerTopic;
    }
}
