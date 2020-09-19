package finance.modelling.data.ingestfinancialexchangetickers.client.contract;

import finance.modelling.data.ingestfinancialexchangetickers.client.dto.FmpTickerDTO;
import reactor.core.publisher.Flux;

import java.net.URI;

public interface FmpClient {
    Flux<FmpTickerDTO> getAllCompanyTickers(URI resourceUri);
}
