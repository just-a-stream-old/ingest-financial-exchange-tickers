package finance.modelling.data.ingestfinancialexchangetickers.api.publisher;

import finance.modelling.data.ingestfinancialexchangetickers.api.publisher.model.EodExchange;

public class KafkaPublisherEodExchangeImpl implements KafkaPublisher<EodExchange> {

    public void publishMessage(String topic, EodExchange payload) {

    }
}
