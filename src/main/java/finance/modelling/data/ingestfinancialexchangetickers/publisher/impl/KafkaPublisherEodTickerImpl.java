package finance.modelling.data.ingestfinancialexchangetickers.publisher.impl;

import finance.modelling.data.ingestfinancialexchangetickers.client.dto.EodTickerDTO;
import finance.modelling.data.ingestfinancialexchangetickers.publisher.contract.KafkaPublisher;
import finance.modelling.fmcommons.data.logging.LogPublisher;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

import static finance.modelling.fmcommons.data.helper.api.publisher.PublisherHelper.buildProducerRecordWithTraceIdHeader;

@Component
public class KafkaPublisherEodTickerImpl implements KafkaPublisher<EodTickerDTO> {

    private final KafkaTemplate<String, Object> template;

    public KafkaPublisherEodTickerImpl(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    public void publishMessage(String topic, EodTickerDTO payload) {
        UUID traceId = UUID.randomUUID();
        template.send(buildProducerRecordWithTraceIdHeader(topic, payload.getSymbol(),payload, traceId));
        LogPublisher.logInfoDataItemSent(EodTickerDTO.class, topic, List.of(traceId));
    }
}
