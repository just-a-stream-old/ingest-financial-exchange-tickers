package finance.modelling.data.ingestfinancialexchangetickers.publisher.model;

import lombok.Data;

@Data
public class EodExchange {
    private String symbol;
    private String name;
    private String country;
    private String exchange;
    private String currency;
    private String type;
}
