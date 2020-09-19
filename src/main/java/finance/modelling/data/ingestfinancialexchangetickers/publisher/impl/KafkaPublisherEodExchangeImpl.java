package finance.modelling.data.ingestfinancialexchangetickers.publisher.impl;

import finance.modelling.data.ingestfinancialexchangetickers.publisher.contract.KafkaPublisher;
import finance.modelling.fmcommons.data.logging.LogPublisher;
import finance.modelling.fmcommons.data.schema.eod.dto.EodExchangeDTO;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.UUID;

import static finance.modelling.fmcommons.data.helper.api.publisher.PublisherHelper.buildProducerRecordWithTraceIdHeader;

@Component
public class KafkaPublisherEodExchangeImpl implements KafkaPublisher<EodExchangeDTO> {

    private final KafkaTemplate<String, Object> template;

    public KafkaPublisherEodExchangeImpl(KafkaTemplate<String, Object> template) {
        this.template = template;
    }

    public void publishMessage(String topic, EodExchangeDTO payload) {
        String traceId = UUID.randomUUID().toString();
        template.send(buildProducerRecordWithTraceIdHeader(topic, payload.getCode(),payload, traceId));
        LogPublisher.logInfoDataItemSent(EodExchangeDTO.class, topic, traceId);
    }
}
