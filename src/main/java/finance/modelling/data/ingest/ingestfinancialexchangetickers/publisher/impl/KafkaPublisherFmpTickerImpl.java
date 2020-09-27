package finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.impl;

import finance.modelling.data.ingest.ingestfinancialexchangetickers.publisher.contract.KafkaPublisher;
import finance.modelling.fmcommons.data.logging.LogPublisher;
import finance.modelling.fmcommons.data.schema.fmp.dto.FmpTickerDTO;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static finance.modelling.fmcommons.data.helper.api.publisher.PublisherHelper.buildProducerRecordWithTraceIdHeader;

@Component
public class KafkaPublisherFmpTickerImpl implements KafkaPublisher<FmpTickerDTO> {


    private final KafkaTemplate<String, Object> template;

    public KafkaPublisherFmpTickerImpl(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    public void publishMessage(String topic, FmpTickerDTO payload) {
        String traceId = UUID.randomUUID().toString();
        template.send(buildProducerRecordWithTraceIdHeader(topic, payload.getSymbol(), payload, traceId));
        LogPublisher.logInfoDataItemSent(FmpTickerDTO.class, topic, traceId);
    }

    public void publishMessage(String topic, String payload) {
        String traceId = UUID.randomUUID().toString();
        template.send(buildProducerRecordWithTraceIdHeader(topic, payload, payload, traceId));
        LogPublisher.logInfoDataItemSent(FmpTickerDTO.class, topic, traceId);
    }
}
