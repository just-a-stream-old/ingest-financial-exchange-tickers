package finance.modelling.data.ingestfinancialexchangetickers.client.impl;

import finance.modelling.fmcommons.data.helper.client.FModellingClientHelper;
import finance.modelling.fmcommons.data.schema.fmp.dto.FmpTickerDTO;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;
import reactor.util.retry.Retry;

import java.net.URI;
import java.time.Duration;

@Component
public class FmpClientImpl {

    private final WebClient client;
    private final FModellingClientHelper fmHelper;

    public FmpClientImpl(WebClient client, FModellingClientHelper fmHelper) {
        this.client = client;
        this.fmHelper = fmHelper;
    }

    public Flux<FmpTickerDTO> getAllCompanyTickers(URI resourceUri) {
        return client
                .get()
                .uri(resourceUri)
                .retrieve()
                .bodyToFlux(FmpTickerDTO.class)
                .onErrorMap(fmHelper::returnTechnicalException)
                .retryWhen(getRetry());
    }

    protected Retry getRetry() {
        return Retry
                .backoff(3, Duration.ofMillis(4000000))
                .filter(fmHelper::isRetryableException);
    }
}
