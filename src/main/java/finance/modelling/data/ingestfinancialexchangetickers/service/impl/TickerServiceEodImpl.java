package finance.modelling.data.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingestfinancialexchangetickers.api.consumer.KafkaConsumerEodExchangeImpl;
import finance.modelling.data.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherEodTickerImpl;
import finance.modelling.data.ingestfinancialexchangetickers.service.contract.TickerService;
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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static finance.modelling.fmcommons.data.exception.ExceptionParser.*;
import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;
import static finance.modelling.fmcommons.data.logging.LogConsumer.determineTraceIdFromHeaders;

@Service
@Slf4j
public class TickerServiceEodImpl implements TickerService {

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
            KafkaConsumerEodExchangeImpl kafkaConsumer,
            @Value("${kafka.bindings.publisher.eod.eodExchanges}") String inputExchangeTopic,
            EodHistoricalClient eodHistoricalClient,
            KafkaPublisherEodTickerImpl kafkaPublisher,
            @Value("${kafka.bindings.publisher.eod.eodTickers}") String outputTickerTopic,
            @Value("${client.eod.security.key}") String eodApiKey,
            @Value("${client.eod.baseUrl}") String eodBaseUrl,
            @Value("${client.eod.resource.eodTickers}") String tickerResourceUrl,
            @Value("${client.eod.request.delay.ms}") Long requestDelayMs) {
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
                        error -> respondToErrorType(error, exchangeCode)
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

    private void respondToErrorType(Throwable error, String exchangeCode) {
        List<String> responseToError = new LinkedList<>();

        if (isClientDailyRequestLimitReached(error)) {
            responseToError.add("Scheduled retry...");
        }
        else if (isKafkaException(error)) {
            responseToError.add("Print stacktrace");
            error.printStackTrace();
        }
        else if (isSaslAuthentificationException(error)) {
            responseToError.add("Print error message");
            log.error(error.getMessage());
        }
        else {
            responseToError.add("Default");
        }
        LogClient.logErrorFailedToReceiveDataItem(
                "Unknown", EodTickerDTO.class, error, logResourcePath, responseToError,
                Map.of("exchangeCode", exchangeCode));
    }
}
