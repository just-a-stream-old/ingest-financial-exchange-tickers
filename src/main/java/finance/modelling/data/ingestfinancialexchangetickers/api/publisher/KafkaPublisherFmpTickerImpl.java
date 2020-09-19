package finance.modelling.data.ingestfinancialexchangetickers.api.publisher;

import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.model.FmpTicker;
import org.springframework.stereotype.Component;

@Component
public class KafkaPublisherFmpTickerImpl implements KafkaPublisher<FmpTicker> {


    public void publishMessage(String topic, FmpTicker payload) {

    }
}
