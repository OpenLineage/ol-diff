/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static io.openlineage.client.OpenLineageClientUtils.newObjectMapper;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.openlineage.client.OpenLineage.DatasetFacet;
import io.openlineage.client.OpenLineage.InputDataset;
import io.openlineage.client.OpenLineage.InputDatasetFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.utils.DatasetIdentifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class InputDatasetHelper {

  Context context;
  ObjectMapper mapper = newObjectMapper();
  SparkActionId sparkActionId;

  InputDatasetHelper(Context context, SparkActionId sparkActionId) {
    this.context = context;
    this.sparkActionId = sparkActionId;
  }

  public List<InputDataset> prevInputs(DatasetIdentifier identifier) {
    return context.getPrevEvents().stream()
        .filter(e -> e.getRun() != null)
        .filter(e -> sparkActionId.prevRunId.equals(e.getRun().getRunId()))
        .map(RunEvent::getInputs)
        .flatMap(List::stream)
        .filter(d -> d.getNamespace().equals(identifier.getNamespace()))
        .filter(d -> d.getName().equals(identifier.getName()))
        .collect(Collectors.toList());
  }

  public List<InputDataset> nextInputs(DatasetIdentifier identifier) {
    return context.getNextEvents().stream()
        .filter(e -> e.getRun() != null)
        .filter(e -> sparkActionId.nextRunId.equals(e.getRun().getRunId()))
        .map(RunEvent::getInputs)
        .flatMap(List::stream)
        .filter(d -> d.getNamespace().equals(identifier.getNamespace()))
        .filter(d -> d.getName().equals(identifier.getName()))
        .collect(Collectors.toList());
  }

  public Map<DatasetIdentifier, List<String>> prevFacets() {
    // get list of datasets from prev events
    Map<DatasetIdentifier, List<String>> map =
        ids().stream()
            .collect(
                Collectors.toMap(
                    d -> d, d -> new ArrayList<>(mergedDatasetFacets(prevInputs(d)).keySet())));
    map.values().removeIf(Collection::isEmpty);
    return map;
  }

  public Map<DatasetIdentifier, List<String>> prevInputFacets() {
    // get list of datasets from prev events
    Map<DatasetIdentifier, List<String>> map =
        ids().stream()
            .collect(
                Collectors.toMap(
                    d -> d,
                    d -> new ArrayList<>(mergedInputDatasetFacets(prevInputs(d)).keySet())));
    map.values().removeIf(Collection::isEmpty);
    return map;
  }

  private Set<DatasetIdentifier> ids() {
    return context.getPrevEvents().stream()
        .filter(e -> e.getRun() != null)
        .filter(e -> sparkActionId.prevRunId.equals(e.getRun().getRunId()))
        .flatMap(i -> i.getInputs().stream())
        .map(d -> new DatasetIdentifier(d.getName(), d.getNamespace()))
        .collect(Collectors.toSet());
  }

  public Map<String, DatasetFacet> mergedDatasetFacets(List<InputDataset> datasetList) {
    return datasetList.stream()
        .map(
            d ->
                mapper.convertValue(
                    d.getFacets(), new TypeReference<Map<String, DatasetFacet>>() {}))
        .reduce(this::mergeMap)
        .orElse(Collections.EMPTY_MAP);
  }

  public Map<String, InputDatasetFacet> mergedInputDatasetFacets(List<InputDataset> datasetList) {
    return datasetList.stream()
        .map(
            d ->
                mapper.convertValue(
                    d.getInputFacets(), new TypeReference<Map<String, InputDatasetFacet>>() {}))
        .reduce(this::mergeMap)
        .orElse(Collections.EMPTY_MAP);
  }

  private <T> Map<String, T> mergeMap(Map<String, T> m1, Map<String, T> m2) {
    m1.putAll(m2);
    return m1;
  }
}
