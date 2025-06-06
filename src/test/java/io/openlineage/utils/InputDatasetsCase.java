/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.utils.Config.FacetConfig;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("Verify input dataset facets")
@Slf4j
public class InputDatasetsCase {

  private static Context context;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
  }

  @ParameterizedTest
  @MethodSource("sparkActionIds")
  @DisplayName("Verify input dataset names and namespaces")
  void verifyInputNames(SparkActionId sparkActionId) {
    Set<Dataset> prev =
        context.getPrevEvents().stream()
            .filter(e -> sparkActionId.prevRunId.equals(e.getRun().getRunId()))
            .flatMap(e -> e.getInputs().stream())
            .collect(Collectors.toSet());

    Set<Dataset> next =
        context.getNextEvents().stream()
            .filter(e -> sparkActionId.nextRunId.equals(e.getRun().getRunId()))
            .flatMap(e -> e.getInputs().stream())
            .collect(Collectors.toSet());

    // check all from prev are in next
    for (Dataset dataset : prev) {
      // for each prev dataset there should be a next dataset
      boolean found = false;
      for (Dataset el : next) {
        found = found || DatasetUtils.areSameName(dataset, el);
      }
      assertThat(found)
          .describedAs("Dataset should have the same name {}", dataset.getName())
          .isTrue();
    }

    // check the opposite way
    for (Dataset dataset : next) {
      // for each prev dataset there should be a next dataset
      boolean found = false;
      for (Dataset el : prev) {
        found = found || DatasetUtils.areSameName(dataset, el);
      }
      assertThat(found)
          .describedAs("Dataset should have the same name {}", dataset.getName())
          .isTrue();
    }
  }

  @ParameterizedTest
  @MethodSource("prevDatasetFacets")
  @DisplayName("Verify dataset facet: {} {}")
  void verifyDatasetFacet(
      InputDatasetHelper datasetHelper, DatasetIdentifier di, String facetName) {
    if (di == null) {
      assertThat(true).describedAs("No input datasets found").isTrue();
      return;
    }

    DatasetFacet prevFacet =
        datasetHelper.mergedDatasetFacets(datasetHelper.prevInputs(di)).get(facetName);

    DatasetFacet nextFacet =
        datasetHelper.mergedDatasetFacets(datasetHelper.nextInputs(di)).get(facetName);

    assertThat(nextFacet)
        .overridingErrorMessage("Next facets should contain facet: " + facetName)
        .isNotNull();

    Optional<FacetConfig> facetConfig =
        Optional.ofNullable(context.getConfig().getDataset()).map(m -> m.get(facetName));
    Map<String, Object> checkedPrevProperties =
        prevFacet.getAdditionalProperties().entrySet().stream()
            .filter(e -> facetConfig.map(f -> !f.isPropertyIgnored(e.getKey())).orElse(true))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev input facet additional properties")
        .containsAllEntriesOf(checkedPrevProperties);
  }

  @ParameterizedTest
  @MethodSource("prevInputFacets")
  @DisplayName("Verify input dataset facets {}")
  void verifyInputDatasetFacet(
      InputDatasetHelper datasetHelper, DatasetIdentifier di, String facetName) {
    if (di == null) {
      assertThat(true).describedAs("No input datasets found").isTrue();
      return;
    }
    InputDatasetFacet prevFacet =
        datasetHelper.mergedInputDatasetFacets(datasetHelper.prevInputs(di)).get(facetName);

    InputDatasetFacet nextFacet =
        datasetHelper.mergedInputDatasetFacets(datasetHelper.nextInputs(di)).get(facetName);

    assertThat(nextFacet)
        .overridingErrorMessage("Next facets should contain facet: " + facetName)
        .isNotNull();

    Optional<FacetConfig> facetConfig =
        Optional.ofNullable(context.getConfig().getInputDataset()).map(m -> m.get(facetName));
    log.info("FacetConfig: {} {}", facetName, facetConfig);
    Map<String, Object> checkedPrevProperties =
        prevFacet.getAdditionalProperties().entrySet().stream()
            .filter(e -> facetConfig.map(f -> !f.isPropertyIgnored(e.getKey())).orElse(true))
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev input facet additional properties {}", facetName)
        .containsAllEntriesOf(checkedPrevProperties);
  }

  private static Stream<Arguments> prevInputFacets() {
    List<Arguments> arguments = new ArrayList<>();
    Context context = Context.loadContext();

    context
        .getSparkActionsIds()
        .forEach(
            sparkActionId -> {
              InputDatasetHelper datasetHelper = new InputDatasetHelper(context, sparkActionId);
              arguments.addAll(
                  datasetHelper.prevInputFacets().entrySet().stream()
                      .flatMap(
                          entry ->
                              entry.getValue().stream()
                                  .filter(
                                      facet ->
                                          Optional.ofNullable(context.getConfig().getInputDataset())
                                              .filter(m -> m.containsKey(facet))
                                              .filter(m -> m.get(facet).isDisabled())
                                              .isEmpty())
                                  .map(
                                      facetName ->
                                          Arguments.of(
                                              Named.of(
                                                  "Run: " + sparkActionId.prevRunId, datasetHelper),
                                              Named.of(
                                                  "dataset " + entry.getKey().getName(),
                                                  entry.getKey()),
                                              Named.of("Facet " + facetName, facetName))))
                      .toList());
            });

    if (arguments.isEmpty()) {
      return Stream.of(Arguments.of(Named.of("No input facets to verify", null), null, null));
    } else {
      return arguments.stream();
    }
  }

  private static Stream<Arguments> prevDatasetFacets() {
    List<Arguments> arguments = new ArrayList<>();
    Context context = Context.loadContext();

    context
        .getSparkActionsIds()
        .forEach(
            sparkActionId -> {
              InputDatasetHelper datasetHelper = new InputDatasetHelper(context, sparkActionId);
              arguments.addAll(
                  datasetHelper.prevFacets().entrySet().stream()
                      .flatMap(
                          entry ->
                              entry.getValue().stream()
                                  .filter(
                                      facet ->
                                          Optional.ofNullable(context.getConfig().getDataset())
                                              .filter(m -> m.containsKey(facet))
                                              .filter(m -> m.get(facet).isDisabled())
                                              .isEmpty())
                                  .map(
                                      facetName ->
                                          Arguments.of(datasetHelper, entry.getKey(), facetName)))
                      .toList());
            });

    if (arguments.isEmpty()) {
      return Stream.of(Arguments.of(Named.of("No facets to verify", null), null, null));
    } else {
      return arguments.stream();
    }
  }

  private static Stream<Arguments> sparkActionIds() {
    return context.getSparkActionsIds().stream()
        .map(
            sparkActionId ->
                Arguments.of(
                    Named.of("Prev RunId " + sparkActionId.prevRunId.toString(), sparkActionId)));
  }
}
