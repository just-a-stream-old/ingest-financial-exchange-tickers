package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingest.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherEodExchangeImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config.EodApiConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config.TopicConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.contract.ExchangeService;
import finance.modelling.fmcommons.data.helper.client.EodHistoricalClientHelper;
import finance.modelling.fmcommons.data.logging.LogClient;
import finance.modelling.fmcommons.data.schema.eod.dto.EodExchangeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.URI;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

@Service
@Slf4j
public class ExchangeServiceEodImpl implements ExchangeService {

    private final EodHistoricalClientHelper eodHelper;
    private final EodHistoricalClient eodHistoricalClient;
    private final EodApiConfig eodApi;
    private final KafkaPublisherEodExchangeImpl kafkaPublisher;
    private final TopicConfig topics;
    private final String logResourcePath;

    public ExchangeServiceEodImpl(
            EodHistoricalClientHelper eodHelper,
            EodHistoricalClient eodHistoricalClient,
            EodApiConfig eodApi,
            KafkaPublisherEodExchangeImpl kafkaPublisher,
            TopicConfig topics) {
        this.eodHelper = eodHelper;
        this.eodHistoricalClient = eodHistoricalClient;
        this.eodApi = eodApi;
        this.kafkaPublisher = kafkaPublisher;
        this.topics = topics;
        this.logResourcePath = buildResourcePath(eodApi.getBaseUrl(), eodApi.getExchangeResourceUrl());
    }

    public void ingestAllExchanges() {
        eodHistoricalClient
                .getAllExchanges(buildExchangesUri())
                .delayElements(eodApi.getRequestDelayMs())
                .doOnNext(exchange -> kafkaPublisher.publishMessage(topics.getEodExchangeTopic(), exchange))
                .subscribe(
                        exchange -> LogClient.logInfoDataItemReceived(exchange.getCode(), EodExchangeDTO.class, logResourcePath),
                        error -> eodHelper.respondToErrorType("Unknown", EodExchangeDTO.class, error, logResourcePath),
                        () -> LogClient.logInfoProcessComplete("ingestAllExchanges()")
                );
    }

    protected URI buildExchangesUri() {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(eodApi.getBaseUrl())
                .path(eodApi.getExchangeResourceUrl())
                .queryParam("api_token", eodApi.getApiKey())
                .queryParam("fmt", "json")
                .build()
                .toUri();
    }
}
