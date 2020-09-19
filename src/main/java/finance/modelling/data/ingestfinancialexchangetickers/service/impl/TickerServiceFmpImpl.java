package finance.modelling.data.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherFmpTickerImpl;
import finance.modelling.data.ingestfinancialexchangetickers.client.dto.FmpTickerDTO;
import finance.modelling.data.ingestfinancialexchangetickers.client.impl.FmpClientImpl;
import finance.modelling.data.ingestfinancialexchangetickers.service.contract.TickerService;
import finance.modelling.fmcommons.data.logging.LogClient;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

import static finance.modelling.fmcommons.data.exception.ExceptionParser.isKafkaException;
import static finance.modelling.fmcommons.data.exception.ExceptionParser.isSaslAuthentificationException;
import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

@Service
@Slf4j
public class TickerServiceFmpImpl implements TickerService {

    private final FmpClientImpl fmpClient;
    private final KafkaPublisherFmpTickerImpl kafkaPublisher;
    private final String outputTickerTopic;
    private final String fmpApiKey;
    private final String fmpBaseUrl;
    private final String allTickersResourceUrl;
    private final String logResourcePath;
    private final Long requestDelayMs;

    public TickerServiceFmpImpl(
            FmpClientImpl fmpClient,
            KafkaPublisherFmpTickerImpl kafkaPublisher,
            @Value("${api.publisher.kafka.bindings.fmp.fmpTickers}") String outputTickerTopic,
            @Value("${client.fmp.security.key}") String fmpApiKey,
            @Value("${client.fmp.baseUrl}") String fmpBaseUrl,
            @Value("${client.fmp.resource.fmpTickers}") String allTickersResourceUrl,
            @Value("${client.fmp.request.delay.ms}") Long requestDelayMs) {
        this.fmpClient = fmpClient;
        this.kafkaPublisher = kafkaPublisher;
        this.outputTickerTopic = outputTickerTopic;
        this.fmpApiKey = fmpApiKey;
        this.fmpBaseUrl = fmpBaseUrl;
        this.allTickersResourceUrl = allTickersResourceUrl;
        this.logResourcePath = buildResourcePath(fmpBaseUrl, allTickersResourceUrl);
        this.requestDelayMs = requestDelayMs;
    }

    public void ingestAllTickers() {
        fmpClient
                .getAllCompanyTickers(buildAllTickersUri())
                .delayElements(Duration.ofMillis(requestDelayMs))
                .doOnNext(ticker -> kafkaPublisher.publishMessage(outputTickerTopic, ticker))
                .subscribe(
                        ticker -> LogClient.logInfoDataItemReceived(ticker.getSymbol(), FmpTickerDTO.class, logResourcePath),
                        this::respondToErrorType,
                        () -> log.info("Process complete: ingestAllTickers().")
                );
    }

    private URI buildAllTickersUri() {
        return UriComponentsBuilder.newInstance()
                .scheme("https")
                .host(fmpBaseUrl)
                .path(allTickersResourceUrl)
                .queryParam("apikey", fmpApiKey)
                .build()
                .toUri();
    }

    protected void respondToErrorType(Throwable error) {
        List<String> responsesToError = new LinkedList<>();

        if (isKafkaException(error)) {
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
        LogClient.logErrorFailedToReceiveDataItem("Unknown", FmpTickerDTO.class, error, logResourcePath, responsesToError);
    }
}
