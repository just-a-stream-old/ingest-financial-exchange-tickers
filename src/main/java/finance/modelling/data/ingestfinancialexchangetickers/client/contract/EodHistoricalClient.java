package finance.modelling.data.ingestfinancialexchangetickers.client.contract;

import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodExchangeDTO;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodTickerDTO;
import reactor.core.publisher.Flux;

import java.net.URI;

public interface EodHistoricalClient {
    Flux<EodExchangeDTO> getAllExchanges(URI resourceUri);
    Flux<EodTickerDTO> getAllExchangeTickers(URI resourceUri);
}
