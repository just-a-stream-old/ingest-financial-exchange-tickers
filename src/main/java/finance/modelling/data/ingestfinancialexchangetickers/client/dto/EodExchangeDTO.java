package finance.modelling.data.ingestfinancialexchangetickers.client.dto;

import com.fasterxml.jackson.annotation.JsonProperty;
import lombok.Data;

@Data
public class EodExchangeDTO {
    @JsonProperty("Name") private String name;
    @JsonProperty("Code") private String code;
    @JsonProperty("OperatingMIC") private String operatingMICs;
    @JsonProperty("Country") private String country;
    @JsonProperty("Currency") private String currency;
}
