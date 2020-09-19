package finance.modelling.data.ingestfinancialexchangetickers.client.impl;

import finance.modelling.data.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodExchangeDTO;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodTickerDTO;
import finance.modelling.fmcommons.data.helper.client.EodHistoricalClientHelper;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;

@Component
public class EodHistoricalClientImpl implements EodHistoricalClient {

    private final WebClient client;
    private final EodHistoricalClientHelper eodHelper;

    public EodHistoricalClientImpl(WebClient client, EodHistoricalClientHelper eodHelper) {
        this.client = client;
        this.eodHelper = eodHelper;
    }

    public Flux<EodExchangeDTO> getAllExchanges(URI resourceUri) {
        return client
                .get()
                .uri(resourceUri)
                .retrieve()
                .bodyToFlux(EodExchangeDTO.class)
                .onErrorMap(eodHelper::returnTechnicalException)
                .retryWhen(getRetry());
    }

    public Flux<EodTickerDTO> getAllExchangeTickers(URI resourceUri) {
        return client
                .get()
                .uri(resourceUri)
                .retrieve()
                .bodyToFlux(EodTickerDTO.class)
                .onErrorMap(eodHelper::returnTechnicalException)
                .retryWhen(getRetry());
    }

    protected Retry getRetry() {
        return Retry
                .backoff(10, Duration.ofMillis(200))
                // Todo: Add something after retry?
                .doAfterRetry(Object::toString)
                .filter(eodHelper::isNotRetryableException);
    }
}
