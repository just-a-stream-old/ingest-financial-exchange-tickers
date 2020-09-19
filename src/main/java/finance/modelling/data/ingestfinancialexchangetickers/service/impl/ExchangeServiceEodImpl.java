package finance.modelling.data.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.KafkaPublisher;
import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.KafkaPublisherEodExchangeImpl;
import finance.modelling.data.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodExchangeDTO;
import finance.modelling.data.ingestfinancialexchangetickers.service.contract.ExchangeService;
import finance.modelling.fmcommons.data.logging.LogClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import static finance.modelling.fmcommons.data.exception.ExceptionParser.*;
import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

@Service
@Slf4j
public class ExchangeServiceEodImpl implements ExchangeService {

    private final EodHistoricalClient eodHistoricalClient;
    private final KafkaPublisherEodExchangeImpl kafkaPublisher;
    private final String eodApiKey;
    private final String eodBaseUrl;
    private final String exchangesResourceUrl;
    private final String logResourcePath;
    private final Long requestDelayMs;

    public ExchangeServiceEodImpl(
            EodHistoricalClient eodHistoricalClient,
            KafkaPublisherEodExchangeImpl kafkaPublisher,
            @Value("${client.api.eod.security.key}") String eodApiKey,
            @Value("${client.api.eod.baseUrl}") String eodBaseUrl,
            @Value("${client.api.eod.resource.eodExchanges}") String exchangesResourceUrl,
            @Value("${client.api.request.delay.ms}") Long requestDelayMs) {
        this.eodHistoricalClient = eodHistoricalClient;
        this.kafkaPublisher = kafkaPublisher;
        this.eodApiKey = eodApiKey;
        this.eodBaseUrl = eodBaseUrl;
        this.exchangesResourceUrl = exchangesResourceUrl;
        this.logResourcePath = buildResourcePath(eodBaseUrl, exchangesResourceUrl);
        this.requestDelayMs = requestDelayMs;
    }

    public void ingestAllExchanges() {
        eodHistoricalClient
                .getAllExchanges(buildExchangesUri())
                .delayElements(Duration.ofMillis(requestDelayMs))
                .doOnNext(exchange -> kafkaPublisher.publishMessage("tickers-eg2-topic", exchange))
                .subscribe(
                        exchange -> LogClient.logInfoDataItemReceived(
                                exchange.getCode(), EodExchangeDTO.class, exchangesResourceUrl),
                        this::respondToErrorType,
                        () -> log.info("Process complete: ingestAllExchanges().")
                );
    }

    protected URI buildExchangesUri() {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(eodBaseUrl)
                .path(exchangesResourceUrl)
                .queryParam("api_token", eodApiKey)
                .queryParam("fmt", "json")
                .build()
                .toUri();
    }

    protected void respondToErrorType(Throwable error) {
        List<String> responsesToError = new LinkedList<>();

        if (isClientDailyRequestLimitReached(error)) {
            // Todo: Implement stateful retry system when max requests limit reached
            responsesToError.add("Scheduled retry...");
        }
        else if (isKafkaException(error)) {
            responsesToError.add("Print stacktrace");
            error.printStackTrace();
        }
        else if (isSaslAuthentificationException(error)) {
            responsesToError.add("Print error message");
            log.error(error.getMessage());
        }
        else {
            responsesToError.add("Default");
        }
        LogClient.logErrorFailedToReceiveDataItem("Unknown", EodExchangeDTO.class, error, logResourcePath, responsesToError);
    }
}
