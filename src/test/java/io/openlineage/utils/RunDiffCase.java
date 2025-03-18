/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunFacet;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@DisplayName("Verify run facets")
public class RunDiffCase {

  private static Context context;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
  }

  @ParameterizedTest
  @MethodSource("prevRunFacets")
  @DisplayName("Verify run facet {}")
  void verifyRunFacets(
      String runDesc, String prevFacetName, RunFacet prevFacet, RunFacet nextFacet) {
    assertThat(nextFacet)
        .overridingErrorMessage(
            "Next run facets should contain prev run prevFacet: " + prevFacetName)
        .isNotNull();

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev run {} facet additional properties", runDesc)
        .containsAllEntriesOf(prevFacet.getAdditionalProperties());
  }

  private static Stream<Arguments> prevRunFacets() {
    List<Arguments> arguments = new ArrayList<>();
    context
        .getSparkActionsIds()
        .forEach(
            sparkActionId -> {
              RunHelper runHelper = new RunHelper(context, sparkActionId);
              arguments.addAll(
                  runHelper.prevMergedFacets().entrySet().stream()
                      .filter(
                          facet ->
                              Optional.ofNullable(context.getConfig().getRun())
                                  .filter(m -> m.containsKey(facet.getKey()))
                                  .filter(m -> m.get(facet.getKey()).isDisabled())
                                  .isEmpty())
                      .map(
                          e ->
                              Arguments.of(
                                  "Job " + sparkActionId.getJobName(),
                                  Named.of("compare run facet " + e.getKey(), e.getKey()),
                                  Named.of("with prev run facet", e.getValue()),
                                  Named.of(
                                      "and next run facet",
                                      runHelper.nextMergedFacets().getOrDefault(e.getKey(), null))))
                      .toList());
            });

    return arguments.stream();
  }
}
