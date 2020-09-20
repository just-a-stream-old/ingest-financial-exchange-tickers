package finance.modelling.data.ingestfinancialexchangetickers.service.impl;

import finance.modelling.data.ingestfinancialexchangetickers.client.contract.EodHistoricalClient;
import finance.modelling.data.ingestfinancialexchangetickers.publisher.impl.KafkaPublisherEodExchangeImpl;
import finance.modelling.data.ingestfinancialexchangetickers.service.contract.ExchangeService;
import finance.modelling.fmcommons.data.helper.client.EodHistoricalClientHelper;
import finance.modelling.fmcommons.data.logging.LogClient;
import finance.modelling.fmcommons.data.schema.eod.dto.EodExchangeDTO;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.web.util.UriComponentsBuilder;

import java.net.URI;
import java.time.Duration;

import static finance.modelling.fmcommons.data.logging.LogClient.buildResourcePath;

@Service
@Slf4j
public class ExchangeServiceEodImpl implements ExchangeService {

    private final EodHistoricalClientHelper eodHelper;
    private final EodHistoricalClient eodHistoricalClient;
    private final KafkaPublisherEodExchangeImpl kafkaPublisher;
    private final String outputExchangeTopic;
    private final String eodApiKey;
    private final String eodBaseUrl;
    private final String exchangesResourceUrl;
    private final String logResourcePath;
    private final Long requestDelayMs;

    public ExchangeServiceEodImpl(
            EodHistoricalClientHelper eodHelper,
            EodHistoricalClient eodHistoricalClient,
            KafkaPublisherEodExchangeImpl kafkaPublisher,
            @Value("${kafka.bindings.publisher.eod.eodExchanges}") String outputExchangeTopic,
            @Value("${client.eod.security.key}") String eodApiKey,
            @Value("${client.eod.baseUrl}") String eodBaseUrl,
            @Value("${client.eod.resource.eodExchanges}") String exchangesResourceUrl,
            @Value("${client.eod.request.delay.ms}") Long requestDelayMs) {
        this.eodHelper = eodHelper;
        this.eodHistoricalClient = eodHistoricalClient;
        this.kafkaPublisher = kafkaPublisher;
        this.outputExchangeTopic = outputExchangeTopic;
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
                .doOnNext(exchange -> kafkaPublisher.publishMessage(outputExchangeTopic, exchange))
                .subscribe(
                        exchange -> LogClient.logInfoDataItemReceived(exchange.getCode(), EodExchangeDTO.class, logResourcePath),
                        error -> eodHelper.respondToErrorType("Unknown", EodExchangeDTO.class, error, logResourcePath),
                        () -> LogClient.logInfoProcessComplete("ingestAllExchanges()")
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
}
