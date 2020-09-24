package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherFmpTickerImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.client.impl.FmpClientImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config.FmpApiConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config.TopicConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.contract.TickerService;
import finance.modelling.fmcommons.data.helper.client.FModellingClientHelper;
import finance.modelling.fmcommons.data.logging.LogClient;
import finance.modelling.fmcommons.data.schema.fmp.dto.FmpTickerDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.URI;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

@Service
@Slf4j
public class TickerServiceFmpImpl implements TickerService {

    private final FModellingClientHelper fmHelper;
    private final FmpClientImpl fmpClient;
    private final FmpApiConfig fmpApi;
    private final KafkaPublisherFmpTickerImpl kafkaPublisher;
    private final TopicConfig topics;
    private final String logResourcePath;

    public TickerServiceFmpImpl(
            FModellingClientHelper fmHelper,
            FmpClientImpl fmpClient,
            FmpApiConfig fmpApi,
            KafkaPublisherFmpTickerImpl kafkaPublisher,
            TopicConfig topics) {
        this.fmHelper = fmHelper;
        this.fmpClient = fmpClient;
        this.fmpApi = fmpApi;
        this.kafkaPublisher = kafkaPublisher;
        this.topics = topics;
        this.logResourcePath = buildResourcePath(fmpApi.getBaseUrl(), fmpApi.getTickerResourceUrl());
    }

    public void ingestAllTickers() {
        fmpClient
                .getAllCompanyTickers(buildAllTickersUri())
                .delayElements(fmpApi.getRequestDelayMs())
                .doOnNext(ticker -> kafkaPublisher.publishMessage(topics.getFmpTickerTopic(), ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(ticker.getSymbol(), FmpTickerDTO.class, logResourcePath),
                        error ->  fmHelper.respondToErrorType("Unknown", FmpTickerDTO.class, error, logResourcePath),
                        () -> LogClient.logInfoProcessComplete("ingestAllTickers()")
                );
    }

    private URI buildAllTickersUri() {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(fmpApi.getBaseUrl())
                .path(fmpApi.getTickerResourceUrl())
                .queryParam("apikey", fmpApi.getApiKey())
                .build()
                .toUri();
    }
}