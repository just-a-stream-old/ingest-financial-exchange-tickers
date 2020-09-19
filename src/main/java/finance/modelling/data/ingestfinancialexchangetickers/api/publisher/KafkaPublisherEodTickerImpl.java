package finance.modelling.data.ingestfinancialexchangetickers.api.publisher;

import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.KafkaPublisher;
import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.model.EodTicker;

public class KafkaPublisherEodTickerImpl implements KafkaPublisher<EodTicker> {


    public void publishMessage(String topic, EodTicker payload) {

    }
}
