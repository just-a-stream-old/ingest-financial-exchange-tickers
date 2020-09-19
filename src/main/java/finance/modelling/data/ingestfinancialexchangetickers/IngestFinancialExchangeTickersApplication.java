package finance.modelling.data.ingestfinancialexchangetickers;

import finance.modelling.data.ingestfinancialexchangetickers.service.impl.ExchangeServiceEodImpl;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import javax.annotation.PostConstruct;

@SpringBootApplication
public class IngestFinancialExchangeTickersApplication {

	@Autowired private ExchangeServiceEodImpl exchangeService;

	public static void main(String[] args) {
		SpringApplication.run(IngestFinancialExchangeTickersApplication.class, args);
	}

	@PostConstruct
	void run() {
		exchangeService.ingestAllExchanges();
	}

}
