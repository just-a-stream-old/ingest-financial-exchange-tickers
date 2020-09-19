package finance.modelling.data.ingestfinancialexchangetickers.client.config;

import finance.modelling.fmcommons.data.helper.client.EodHistoricalClientHelper;
import finance.modelling.fmcommons.data.helper.client.FModellingClientHelper;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.reactive.function.client.ExchangeStrategies;
import org.springframework.web.reactive.function.client.WebClient;

@Configuration
public class ClientConfig {

    @Bean
    public WebClient getWebClientBuilder() {
        return WebClient.builder()
                .exchangeStrategies(ExchangeStrategies.builder()
                        .codecs(configurer -> configurer
                                .defaultCodecs()
                                .maxInMemorySize(16 * 1024 * 1024)
                        )
                        .build()
                )
                .build();
    }

    @Bean
    public EodHistoricalClientHelper eodHistoricalClientHelper() {
        return new EodHistoricalClientHelper();
    }

    @Bean
    public FModellingClientHelper fModellingClientHelper() {
        return new FModellingClientHelper();
    }
}
