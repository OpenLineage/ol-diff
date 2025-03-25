package io.openlineage.utils;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.Map;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Config {

  @JsonProperty("dataset")
  Map<String, FacetConfig> dataset;

  @JsonProperty("job")
  Map<String, FacetConfig> job;

  @JsonProperty("run")
  Map<String, FacetConfig> run;

  @JsonProperty("inputDataset")
  Map<String, FacetConfig> inputDataset;

  @JsonProperty("outputDataset")
  Map<String, FacetConfig> outputDataset;

  @Setter
  @NoArgsConstructor
  static class FacetConfig {
    Boolean disabled;

    @Getter String[] ignoredProperties;

    public boolean isDisabled() {
      return disabled != null && disabled;
    }

    public boolean isPropertyIgnored(String property) {
      if (ignoredProperties == null) {
        return false;
      }
      for (String ignoredProperty : ignoredProperties) {
        if (ignoredProperty.equals(property)) {
          return true;
        }
      }
      return false;
    }
  }
}
