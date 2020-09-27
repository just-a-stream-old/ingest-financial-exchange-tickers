package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.config.TopicConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherFmpTickerImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.client.impl.FmpClientImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config.FmpApiConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.contract.FmpTickerService;
import finance.modelling.fmcommons.data.helper.client.FModellingClientHelper;
import finance.modelling.fmcommons.data.logging.LogClient;
import finance.modelling.fmcommons.data.schema.fmp.dto.FmpTickerDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.URI;

@Service
@Slf4j
public class FmpTickerServiceFmpImpl implements FmpTickerService {

    private final FModellingClientHelper fmHelper;
    private final FmpClientImpl fmpClient;
    private final FmpApiConfig fmpApi;
    private final KafkaPublisherFmpTickerImpl kafkaPublisher;
    private final TopicConfig topics;

    public FmpTickerServiceFmpImpl(
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
    }

    public void ingestAllQuoteTickers() {
        fmpClient
                .getAllQuoteTickers(buildAllQuoteTickersUri())
                .delayElements(fmpApi.getRequestDelayMs())
                .doOnNext(ticker -> kafkaPublisher.publishMessage(topics.getFmpQuoteTickerTopic(), ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(
                                ticker.getSymbol(), FmpTickerDTO.class, fmpApi.getQuoteTickerLogResourcePath()),
                        error ->  fmHelper.respondToErrorType(
                                "Unknown", FmpTickerDTO.class, error, fmpApi.getQuoteTickerLogResourcePath()),
                        () -> LogClient.logInfoProcessComplete("ingestAllQuoteTickers()")
                );
    }

    public void ingestAllFinancialStatementTickers() {
        fmpClient
                .getAllStatementTickers(buildAllFinancialStatementTickersUri())
                .delayElements(fmpApi.getRequestDelayMs())
                .doOnNext(ticker -> kafkaPublisher.publishMessage(topics.getFmpStatementTickerTopic(), ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(
                                ticker, String.class, fmpApi.getStatementTickerLogResourcePath()),
                        error ->  fmHelper.respondToErrorType(
                                "Unknown", String.class, error, fmpApi.getStatementTickerLogResourcePath()),
                        () -> LogClient.logInfoProcessComplete("ingestAllFinancialStatementTickers()")
                );
    }

    private URI buildAllQuoteTickersUri() {
        return buildAllTickerUriBase()
                .path(fmpApi.getQuoteTickerResourceUrl())
                .build()
                .toUri();
    }

    private UriComponentsBuilder buildAllTickerUriBase() {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(fmpApi.getBaseUrl())
                .queryParam("apikey", fmpApi.getApiKey());
    }

    private URI buildAllFinancialStatementTickersUri() {
        return buildAllTickerUriBase()
                .path(fmpApi.getStatementResourceUrl())
                .build()
                .toUri();
    }
}