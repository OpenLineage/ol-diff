/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.OutputDatasetFacet;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("Verify output dataset facets")
public class OutputDatasetsCase {

  private static Context context;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
  }

  @ParameterizedTest
  @MethodSource("sparkActionIds")
  @DisplayName("Verify output names")
  void verifyOutputNames(SparkActionId sparkActionId) {
    Set<Dataset> prev =
        context.getPrevEvents().stream()
            .filter(e -> sparkActionId.prevRunId.equals(e.getRun().getRunId()))
            .flatMap(e -> e.getOutputs().stream())
            .collect(Collectors.toSet());

    Set<Dataset> next =
        context.getNextEvents().stream()
            .filter(e -> sparkActionId.nextRunId.equals(e.getRun().getRunId()))
            .flatMap(e -> e.getOutputs().stream())
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
      OutputDatasetHelper datasetHelper, DatasetIdentifier di, String facetName) {
    if (di == null) {
      assertThat(true).describedAs("No output datasets found").isTrue();
      return;
    }

    DatasetFacet prevFacet =
        datasetHelper.mergedDatasetFacets(datasetHelper.prevOutputs(di)).get(facetName);

    DatasetFacet nextFacet =
        datasetHelper.mergedDatasetFacets(datasetHelper.nextOutputs(di)).get(facetName);

    assertThat(nextFacet)
        .overridingErrorMessage("Next facets should contain facet: " + facetName)
        .isNotNull();

    Map<String, Object> checkedPrevProperties =
        prevFacet.getAdditionalProperties().entrySet().stream()
            .filter(
                e ->
                    Optional.ofNullable(context.getConfig().getDataset())
                        .filter(m -> m.containsKey(facetName))
                        .isEmpty())
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev output facet additional properties")
        .containsAllEntriesOf(checkedPrevProperties);
  }

  @ParameterizedTest
  @MethodSource("prevOutputFacets")
  @DisplayName("Verify output dataset facets {}")
  void verifyOutputDatasetFacet(
      OutputDatasetHelper datasetHelper, DatasetIdentifier di, String facetName) {
    if (di == null) {
      assertThat(true).describedAs("No output datasets found").isTrue();
      return;
    }
    OutputDatasetFacet prevFacet =
        datasetHelper.mergedOutputDatasetFacets(datasetHelper.prevOutputs(di)).get(facetName);

    OutputDatasetFacet nextFacet =
        datasetHelper.mergedOutputDatasetFacets(datasetHelper.nextOutputs(di)).get(facetName);

    assertThat(nextFacet)
        .overridingErrorMessage("Next facets should contain facet: " + facetName)
        .isNotNull();

    Map<String, Object> checkedPrevProperties =
        prevFacet.getAdditionalProperties().entrySet().stream()
            .filter(
                e ->
                    Optional.ofNullable(context.getConfig().getOutputDataset())
                        .filter(m -> m.containsKey(facetName))
                        .isEmpty())
            .collect(Collectors.toMap(Entry::getKey, Entry::getValue));

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev output facet additional properties")
        .containsAllEntriesOf(checkedPrevProperties);
  }

  private static Stream<Arguments> prevOutputFacets() {
    List<Arguments> arguments = new ArrayList<>();
    Context context = Context.loadContext();

    context
        .getSparkActionsIds()
        .forEach(
            sparkActionId -> {
              OutputDatasetHelper datasetHelper = new OutputDatasetHelper(context, sparkActionId);
              arguments.addAll(
                  datasetHelper.prevOutputFacets().entrySet().stream()
                      .flatMap(
                          entry ->
                              entry.getValue().stream()
                                  .filter(
                                      facet ->
                                          Optional.ofNullable(
                                                  context.getConfig().getOutputDataset())
                                              .filter(m -> m.containsKey(facet))
                                              .filter(m -> m.get(facet).isDisabled())
                                              .isEmpty())
                                  .map(
                                      facetName ->
                                          Arguments.of(
                                              datasetHelper,
                                              Named.of(
                                                  "Dataset " + entry.getKey().getName(),
                                                  entry.getKey()),
                                              Named.of("Facet " + facetName, facetName))))
                      .toList());
            });

    if (arguments.isEmpty()) {
      return Stream.of(Arguments.of(Named.of("No output facets to verify", null), null, null));
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
              OutputDatasetHelper datasetHelper = new OutputDatasetHelper(context, sparkActionId);
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
