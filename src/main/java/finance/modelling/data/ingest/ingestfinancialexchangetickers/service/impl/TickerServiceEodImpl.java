package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingest.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.config.TopicConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherEodTickerImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.api.consumer.KafkaConsumerEodExchangeImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.config.EodApiConfig;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.contract.EodTickerService;
import finance.modelling.fmcommons.data.helper.client.EodHistoricalClientHelper;
import finance.modelling.fmcommons.data.logging.LogClient;
import finance.modelling.fmcommons.data.logging.LogConsumer;
import finance.modelling.fmcommons.data.schema.eod.dto.EodExchangeDTO;
import finance.modelling.fmcommons.data.schema.eod.dto.EodTickerDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;
import java.net.URI;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;
import static finance.modelling.fmcommons.data.logging.LogConsumer.determineTraceIdFromHeaders;

@Service
@Slf4j
public class TickerServiceEodImpl implements EodTickerService {

    private final EodHistoricalClientHelper eodHelper;
    private final KafkaConsumerEodExchangeImpl kafkaConsumer;
    private final TopicConfig topics;
    private final EodHistoricalClient eodHistoricalClient;
    private final EodApiConfig eodApi;
    private final KafkaPublisherEodTickerImpl kafkaPublisher;
    private final String logResourcePath;

    public TickerServiceEodImpl(
            EodHistoricalClientHelper eodHelper,
            KafkaConsumerEodExchangeImpl kafkaConsumer,
            TopicConfig topics,
            EodHistoricalClient eodHistoricalClient,
            EodApiConfig eodApi,
            KafkaPublisherEodTickerImpl kafkaPublisher) {
        this.eodHelper = eodHelper;
        this.kafkaConsumer = kafkaConsumer;
        this.topics = topics;
        this.eodHistoricalClient = eodHistoricalClient;
        this.eodApi = eodApi;
        this.kafkaPublisher = kafkaPublisher;
        this.logResourcePath = buildResourcePath(eodApi.getBaseUrl(), eodApi.getTickerResourceUrl());
    }

    public void ingestAllTickers() {
        kafkaConsumer
                .receiveMessages(topics.getEodExchangeTopic())
                .delayElements(eodApi.getRequestDelayMs())
                .doOnNext(message -> ingestTickersForExchange(message.value().getCode()))
                .subscribe(
                        message -> LogConsumer.logInfoDataItemConsumed(EodExchangeDTO.class, topics.getEodExchangeTopic(),
                                determineTraceIdFromHeaders(message.headers(), topics.getTraceIdHeaderName())),
                        error -> LogConsumer.logErrorFailedToConsumeDataItem(EodExchangeDTO.class, topics.getEodExchangeTopic())
                );
    }

    private void ingestTickersForExchange(String exchangeCode) {
        eodHistoricalClient
                .getAllExchangeTickers(buildExchangeTickersUri(exchangeCode))
                .doOnNext(ticker -> kafkaPublisher.publishMessage(topics.getEodTickerTopic(), ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(ticker.getSymbol(), EodTickerDTO.class, logResourcePath),
                        error -> eodHelper.respondToErrorType("Unknown", EodExchangeDTO.class, error, logResourcePath)
                );
    }

    private URI buildExchangeTickersUri(String exchangeCode) {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(eodApi.getBaseUrl())
                .path(eodApi.getTickerResourceUrl().concat("/").concat(exchangeCode))
                .queryParam("api_token", eodApi.getApiKey())
                .queryParam("fmt", "json")
                .build()
                .toUri();
    }
}