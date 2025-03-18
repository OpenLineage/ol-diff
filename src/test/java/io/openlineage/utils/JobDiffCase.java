/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.utils.Config.IgnoredFacets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
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

@DisplayName("Verify job facets")
public class JobDiffCase {

  private static Context context;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
  }

  @DisplayName("Verifies identical job name")
  @ParameterizedTest
  @MethodSource("sparkActionIds")
  void verifyJobNames(SparkActionId sparkActionId) {
    JobHelper jobHelper = new JobHelper(context, sparkActionId);
    assertThat(new HashSet<>(jobHelper.prevNames()))
        .describedAs("Set of job names from prev events. Should contain a single job name")
        .hasSize(1);

    assertThat(new HashSet<>(jobHelper.nextNames()))
        .describedAs("Set of job names from next events. Should contain a single job name")
        .hasSize(1);

    assertThat(jobHelper.prevNames().get(0))
        .describedAs("Prev job name")
        .isEqualTo(jobHelper.nextNames().get(0));
  }

  @DisplayName("Verifies identical job namespace")
  @ParameterizedTest
  @MethodSource("sparkActionIds")
  void verifyJobNamespace(SparkActionId sparkActionId) {
    JobHelper jobHelper = new JobHelper(context, sparkActionId);
    assertThat(new HashSet<>(jobHelper.prevNamespaces()))
        .describedAs(
            "Set of job namespaces from prev events. Should contain a single job namespace")
        .hasSize(1);

    assertThat(new HashSet<>(jobHelper.nextNamespaces()))
        .describedAs(
            "Set of job namespaces from next events. Should contain a single job namespace")
        .hasSize(1);

    assertThat(jobHelper.prevNamespaces().get(0))
        .describedAs("Prev job name")
        .isEqualTo(jobHelper.nextNamespaces().get(0));
  }

  @ParameterizedTest
  @MethodSource("prevJobFacets")
  @DisplayName("Verify job facet {}")
  void verifyJobFacets(String runId, String prevFacetName, JobFacet prevFacet, JobFacet nextFacet) {
    if (prevFacetName == null) {
      assertThat(true).describedAs("No output datasets found").isTrue();
      return;
    }

    assertThat(nextFacet)
        .overridingErrorMessage(
            "Next job facets should contain prev job prevFacet: " + prevFacetName)
        .isNotNull();

    assertThat(nextFacet.getAdditionalProperties())
        .describedAs("Prev job facet additional properties")
        .containsAllEntriesOf(prevFacet.getAdditionalProperties());
  }

  private static Stream<Arguments> prevJobFacets() {
    List<Arguments> arguments = new ArrayList<>();
    Context context = Context.loadContext();

    context
        .getSparkActionsIds()
        .forEach(
            sparkActionId -> {
              JobHelper jobHelper = new JobHelper(context, sparkActionId);
              Set<Arguments> collected =
                  jobHelper.prevMergedFacets().entrySet().stream()
                      .filter(
                          e ->
                              Optional.ofNullable(context.getConfig().getIgnoredFacets())
                                  .map(IgnoredFacets::getJobFacets)
                                  .filter(ignoredFacets -> ignoredFacets.contains(e.getKey()))
                                  .isEmpty())
                      .map(
                          e ->
                              Arguments.of(
                                  "Prev RunId " + sparkActionId.prevRunId.toString(),
                                  Named.of("Compare job facet=" + e.getKey(), e.getKey()),
                                  Named.of("prev facet", e.getValue()),
                                  Named.of(
                                      "with next",
                                      jobHelper.nextMergedFacets().getOrDefault(e.getKey(), null))))
                      .collect(Collectors.toSet());
              arguments.addAll(collected);
            });

    if (arguments.isEmpty()) {
      return Stream.of(Arguments.of(Named.of("No facets to verify", null), null, null, null));
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
