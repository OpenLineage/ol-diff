package io.openlineage.utils;

import java.util.List;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
public class Config {

  IgnoredFacets ignoredFacets;

  @Getter
  @Setter
  @NoArgsConstructor
  static class IgnoredFacets {
    List<String> runFacets;
    List<String> jobFacets;
    List<String> inputDatasetFacets;
    List<String> outputDatasetFacets;
    List<String> datasetFacets;
  }
}
