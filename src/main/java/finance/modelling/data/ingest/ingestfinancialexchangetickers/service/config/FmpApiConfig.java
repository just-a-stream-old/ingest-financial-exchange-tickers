package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config;

import lombok.Getter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Configuration;

import java.time.Duration;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

@Configuration
@Getter
public class FmpApiConfig {

    private final String baseUrl;
    private final String apiKey;
    private final Duration requestDelayMs;
    private final String quoteTickerResourceUrl;
    private final String quoteTickerLogResourcePath;
    private final String statementResourceUrl;
    private final String statementTickerLogResourcePath;

    public FmpApiConfig(
            @Value("${client.fmp.baseUrl}") String baseUrl,
            @Value("${client.fmp.security.key}") String apiKey,
            @Value("${client.fmp.request.delay.ms}") Long requestDelayMs,
            @Value("${client.fmp.resource.fmpQuoteTickers}") String quoteTickerResourceUrl,
            @Value("${client.fmp.resource.fmpStatementTickers}") String statementResourceUrl) {
        this.baseUrl = baseUrl;
        this.apiKey = apiKey;
        this.requestDelayMs = Duration.ofMillis(requestDelayMs);
        this.quoteTickerResourceUrl = quoteTickerResourceUrl;
        this.quoteTickerLogResourcePath = buildResourcePath(baseUrl, quoteTickerResourceUrl);
        this.statementResourceUrl = statementResourceUrl;
        this.statementTickerLogResourcePath = buildResourcePath(baseUrl, statementResourceUrl);
    }
}
