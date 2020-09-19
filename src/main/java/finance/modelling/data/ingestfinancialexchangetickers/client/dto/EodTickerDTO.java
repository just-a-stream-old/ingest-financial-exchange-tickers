package finance.modelling.data.ingestfinancialexchangetickers.client.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class EodTickerDTO {
    @JsonProperty("Code") private String symbol;
    @JsonProperty("Name") private String name;
    @JsonProperty("Country") private String country;
    @JsonProperty("Exchange") private String exchange;
    @JsonProperty("Currency") private String currency;
    @JsonProperty("Type") private String type;
}
