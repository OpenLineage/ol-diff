/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static io.openlineage.client.OpenLineageClientUtils.newObjectMapper;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineage.Run;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacet;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class RunHelper {

  Context context;
  SparkActionId sparkActionId;

  private Stream<Run> prevRuns() {
    return context.getPrevEvents().stream()
        .map(RunEvent::getRun)
        .filter(Objects::nonNull)
        .filter(e -> sparkActionId.prevRunId.equals(e.getRunId()));
  }

  private Stream<Run> nextRuns() {
    return context.getNextEvents().stream()
        .map(RunEvent::getRun)
        .filter(Objects::nonNull)
        .filter(e -> sparkActionId.nextRunId.equals(e.getRunId()));
  }

  public Map<String, RunFacet> prevMergedFacets() {
    return prevRuns()
        .map(this::toMap)
        .reduce(
            ((map1, map2) -> {
              map1.putAll(map2);
              return map1;
            }))
        .orElse(Collections.EMPTY_MAP);
  }

  public Map<String, RunFacet> nextMergedFacets() {
    return nextRuns()
        .map(this::toMap)
        .reduce(
            ((map1, map2) -> {
              map1.putAll(map2);
              return map1;
            }))
        .orElse(Collections.EMPTY_MAP);
  }

  private Map<String, RunFacet> toMap(Run run) {
    return newObjectMapper()
        .convertValue(run.getFacets(), new TypeReference<Map<String, RunFacet>>() {});
  }
}
