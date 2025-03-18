/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static io.openlineage.client.OpenLineageClientUtils.newObjectMapper;

import com.fasterxml.jackson.core.type.TypeReference;
import io.openlineage.client.OpenLineage.Job;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;

@AllArgsConstructor
public class JobHelper {

  Context context;
  SparkActionId sparkActionId;

  private Stream<Job> prevJobs() {
    return context.getPrevEvents().stream()
        .filter(e -> e.getRun() != null)
        .filter(event -> sparkActionId.prevRunId.equals(event.getRun().getRunId()))
        .map(RunEvent::getJob)
        .filter(Objects::nonNull);
  }

  private Stream<Job> nextJobs() {
    return context.getNextEvents().stream()
        .filter(e -> e.getRun() != null)
        .filter(event -> sparkActionId.nextRunId.equals(event.getRun().getRunId()))
        .map(RunEvent::getJob)
        .filter(Objects::nonNull);
  }

  public List<String> prevNames() {
    return names(prevJobs());
  }

  public List<String> nextNames() {
    return names(nextJobs());
  }

  public List<String> names(Stream<Job> jobs) {
    return jobs.map(Job::getName).collect(Collectors.toList());
  }

  public List<String> prevNamespaces() {
    return namespaces(prevJobs());
  }

  public List<String> nextNamespaces() {
    return namespaces(nextJobs());
  }

  public List<String> namespaces(Stream<Job> jobs) {
    return jobs.map(Job::getNamespace).collect(Collectors.toList());
  }

  public Map<String, JobFacet> prevMergedFacets() {
    return prevJobs()
        .map(this::toMap)
        .reduce(
            ((map1, map2) -> {
              map1.putAll(map2);
              return map1;
            }))
        .orElse(Collections.EMPTY_MAP);
  }

  public Map<String, JobFacet> nextMergedFacets() {
    return nextJobs()
        .map(this::toMap)
        .reduce(
            ((map1, map2) -> {
              map1.putAll(map2);
              return map1;
            }))
        .orElse(Collections.EMPTY_MAP);
  }

  private Map<String, JobFacet> toMap(Job job) {
    return newObjectMapper()
        .convertValue(job.getFacets(), new TypeReference<Map<String, JobFacet>>() {});
  }
}
