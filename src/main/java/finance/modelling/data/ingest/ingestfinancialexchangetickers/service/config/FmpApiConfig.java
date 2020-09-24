package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

@Configuration
@Getter
public class FmpApiConfig {

    private final String baseUrl;
    private final String apiKey;
    private final Duration requestDelayMs;
    private final String tickerResourceUrl;

    public FmpApiConfig(
            @Value("${client.fmp.baseUrl}") String baseUrl,
            @Value("${client.fmp.security.key}") String apiKey,
            @Value("${client.fmp.request.delay.ms}") Long requestDelayMs,
            @Value("${client.fmp.resource.fmpTickers}") String tickerResourceUrl) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.requestDelayMs = Duration.ofMillis(requestDelayMs);
        this.tickerResourceUrl = tickerResourceUrl;
    }
}
