package finance.modelling.data.ingestfinancialexchangetickers.publisher.impl;

import finance.modelling.data.ingestfinancialexchangetickers.publisher.contract.KafkaPublisher;
import finance.modelling.data.ingestfinancialexchangetickers.publisher.model.EodTicker;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublisherEodTickerImpl implements KafkaPublisher<EodTicker> {


    public void publishMessage(String topic, EodTicker payload) {

    }
}
