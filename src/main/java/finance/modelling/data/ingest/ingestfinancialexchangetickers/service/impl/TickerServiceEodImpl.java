package finance.modelling.data.ingest.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingest.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherEodTickerImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.api.consumer.KafkaConsumerEodExchangeImpl;
import finance.modelling.data.ingest.ingestfinancialexchangetickers.service.contract.TickerService;
import finance.modelling.fmcommons.data.helper.client.EodHistoricalClientHelper;
import finance.modelling.fmcommons.data.logging.LogClient;
import finance.modelling.fmcommons.data.logging.LogConsumer;
import finance.modelling.fmcommons.data.schema.eod.dto.EodExchangeDTO;
import finance.modelling.fmcommons.data.schema.eod.dto.EodTickerDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;
import static finance.modelling.fmcommons.data.logging.LogConsumer.determineTraceIdFromHeaders;

@Service
@Slf4j
public class TickerServiceEodImpl implements TickerService {

    private final EodHistoricalClientHelper eodHelper;
    private final KafkaConsumerEodExchangeImpl kafkaConsumer;
    private final String inputExchangeTopic;
    private final EodHistoricalClient eodHistoricalClient;
    private final KafkaPublisherEodTickerImpl kafkaPublisher;
    private final String outputTickerTopic;
    private final String eodApiKey;
    private final String eodBaseUrl;
    private final String tickerResourceUrl;
    private final String logResourcePath;
    private final Long requestDelayMs;

    public TickerServiceEodImpl(
            EodHistoricalClientHelper eodHelper,
            KafkaConsumerEodExchangeImpl kafkaConsumer,
            @Value("${kafka.bindings.publisher.eod.eodExchanges}") String inputExchangeTopic,
            EodHistoricalClient eodHistoricalClient,
            KafkaPublisherEodTickerImpl kafkaPublisher,
            @Value("${kafka.bindings.publisher.eod.eodTickers}") String outputTickerTopic,
            @Value("${client.eod.security.key}") String eodApiKey,
            @Value("${client.eod.baseUrl}") String eodBaseUrl,
            @Value("${client.eod.resource.eodTickers}") String tickerResourceUrl,
            @Value("${client.eod.request.delay.ms}") Long requestDelayMs) {
        this.eodHelper = eodHelper;
        this.kafkaConsumer = kafkaConsumer;
        this.inputExchangeTopic = inputExchangeTopic;
        this.eodHistoricalClient = eodHistoricalClient;
        this.kafkaPublisher = kafkaPublisher;
        this.outputTickerTopic = outputTickerTopic;
        this.eodApiKey = eodApiKey;
        this.eodBaseUrl = eodBaseUrl;
        this.tickerResourceUrl = tickerResourceUrl;
        this.logResourcePath = buildResourcePath(eodBaseUrl, tickerResourceUrl);
        this.requestDelayMs = requestDelayMs;
    }

    public void ingestAllTickers() {
        kafkaConsumer
                .receiveMessages(inputExchangeTopic)
                .delayElements(Duration.ofMillis(requestDelayMs))
                .doOnNext(message -> ingestTickersForExchange(message.value().getCode()))
                .subscribe(
                        message -> LogConsumer.logInfoDataItemConsumed(
                                EodExchangeDTO.class, inputExchangeTopic, determineTraceIdFromHeaders(message.headers())),
                        error -> LogConsumer.logErrorFailedToConsumeDataItem(EodExchangeDTO.class, inputExchangeTopic)
                );
    }

    private void ingestTickersForExchange(String exchangeCode) {
        eodHistoricalClient
                .getAllExchangeTickers(buildExchangeTickersUri(exchangeCode))
                .doOnNext(ticker -> kafkaPublisher.publishMessage(outputTickerTopic, ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(ticker.getSymbol(), EodTickerDTO.class, logResourcePath),
                        error -> eodHelper.respondToErrorType("Unknown", EodExchangeDTO.class, error, logResourcePath)
                );
    }

    private URI buildExchangeTickersUri(String exchangeCode) {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(eodBaseUrl)
                .path(tickerResourceUrl.concat("/").concat(exchangeCode))
                .queryParam("api_token", eodApiKey)
                .queryParam("fmt", "json")
                .build()
                .toUri();
    }
}
