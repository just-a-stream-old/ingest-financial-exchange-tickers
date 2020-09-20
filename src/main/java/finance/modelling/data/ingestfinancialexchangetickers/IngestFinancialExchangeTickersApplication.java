package finance.modelling.data.ingestfinancialexchangetickers;

import finance.modelling.data.ingestfinancialexchangetickers.service.impl.ExchangeServiceEodImpl;
import finance.modelling.data.ingestfinancialexchangetickers.service.impl.TickerServiceEodImpl;
import finance.modelling.data.ingestfinancialexchangetickers.service.impl.TickerServiceFmpImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class IngestFinancialExchangeTickersApplication {

	@Autowired private ExchangeServiceEodImpl exchangeService;
	@Autowired private TickerServiceFmpImpl tickerServiceFmp;
	@Autowired private TickerServiceEodImpl tickerServiceEod;

	public static void main(String[] args) {
		SpringApplication.run(IngestFinancialExchangeTickersApplication.class, args);
	}

	@PostConstruct
	void run() {
		exchangeService.ingestAllExchanges();
		tickerServiceEod.ingestAllTickers();
		tickerServiceFmp.ingestAllTickers();
	}

}
