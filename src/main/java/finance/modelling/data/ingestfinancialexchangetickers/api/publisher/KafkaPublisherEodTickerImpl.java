package finance.modelling.data.ingestfinancialexchangetickers.api.publisher;

import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.model.EodTicker;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublisherEodTickerImpl implements KafkaPublisher<EodTicker> {


    public void publishMessage(String topic, EodTicker payload) {

    }
}
