package finance.modelling.data.ingestfinancialexchangetickers.client.impl;

import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodExchangeDTO;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodTickerDTO;
import org.springframework.stereotype.Component;
import org.springframework.web.reactive.function.client.WebClient;
import reactor.core.publisher.Flux;

import java.net.URI;

@Component
public class EodHistoricalClientImpl {

    private final WebClient client;

    public EodHistoricalClientImpl(WebClient client) {
        this.client = client;
    }

    public Flux<EodExchangeDTO> getAllExchanges(URI resourceUri) {
        return client
                .get()
                .uri(resourceUri)
                .retrieve()
                .bodyToFlux(EodExchangeDTO.class);
    }

    public Flux<EodTickerDTO> getAllExchangeTickers(URI resourceUri) {
        return client
                .get()
                .uri(resourceUri)
                .retrieve()
                .bodyToFlux(EodTickerDTO.class);
    }
}
