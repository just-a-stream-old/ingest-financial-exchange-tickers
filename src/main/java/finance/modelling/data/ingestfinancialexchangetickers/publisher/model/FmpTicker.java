package finance.modelling.data.ingestfinancialexchangetickers.publisher.model;

import lombok.Data;

@Data
public class FmpTicker {
    private String symbol;
    private String name;
    private String price;
    private String exchange;
}
