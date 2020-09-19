package finance.modelling.data.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingestfinancialexchangetickers.api.consumer.contract.KafkaConsumer;
import finance.modelling.data.ingestfinancialexchangetickers.api.consumer.impl.KafkaConsumerEodExchangeImpl;
import finance.modelling.data.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodExchangeDTO;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodTickerDTO;
import finance.modelling.data.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherEodTickerImpl;
import finance.modelling.data.ingestfinancialexchangetickers.service.contract.TickerService;
import finance.modelling.fmcommons.data.logging.LogClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.List;
import java.util.Map;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

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
            @Value("${api.publisher.kafka.bindings.eod.eodExchanges}") String inputExchangeTopic,
            EodHistoricalClient eodHistoricalClient,
            KafkaPublisherEodTickerImpl kafkaPublisher,
            @Value("${api.publisher.kafka.bindings.eod.eodTickers}") String outputTickerTopic,
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
                .map(receiverRecord -> receiverRecord.value().getCode())
                .delayElements(Duration.ofMillis(requestDelayMs))
                .doOnNext(this::ingestTickersForExchange)
                .subscribe(
                        // Todo: Add LogConsumer logging here
                        exchange -> log.info("Successfully queried exchange from kafka: {}", exchange),
                        error -> log.warn(String.format("Error occurred whilst querying exchanges: %s", error))
                );
    }

    private void ingestTickersForExchange(String exchangeCode) {
        eodHistoricalClient
                .getAllExchangeTickers(buildExchangeTickersUri(exchangeCode))
                .doOnNext(ticker -> kafkaPublisher.publishMessage(outputTickerTopic, ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(ticker.getSymbol(), EodTickerDTO.class, logResourcePath),
                        error -> LogClient.logErrorFailedToReceiveDataItem( // Todo: Respond to errors
                                "Unknown", EodTickerDTO.class, error, logResourcePath,
                                List.of("Nothing"), Map.of("exchangeCode", exchangeCode))
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
