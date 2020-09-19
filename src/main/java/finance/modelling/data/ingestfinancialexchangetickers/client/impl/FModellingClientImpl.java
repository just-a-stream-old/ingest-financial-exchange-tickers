package finance.modelling.data.ingestfinancialexchangetickers.client.impl;

import finance.modelling.data.ingestfinancialexchangetickers.client.dto.FmpTickerDTO;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.net.URI;

@Component
public class FModellingClientImpl {

    private final WebClient client;

    public FModellingClientImpl(WebClient client) {
        this.client = client;
    }

    public Flux<FmpTickerDTO> getAllCompanyTickers(URI resourceUri) {
        return client
                .get()
                .uri(resourceUri)
                .retrieve()
                .bodyToFlux(FmpTickerDTO.class);
    }
}
