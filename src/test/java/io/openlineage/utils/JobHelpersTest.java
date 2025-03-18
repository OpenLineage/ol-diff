/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.JobFacet;
import io.openlineage.client.OpenLineage.RunEvent;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("internal-test")
public class JobHelpersTest {

  List<RunEvent> prevEvents = new ArrayList<>();
  List<RunEvent> nextEvents = new ArrayList<>();
  Context context = new Context(prevEvents, nextEvents, new Config());
  SparkActionId sparkActionId = new SparkActionId("spark", UUID.randomUUID(), UUID.randomUUID());
  JobHelper helper = new JobHelper(context, sparkActionId);

  @Test
  @SneakyThrows
  void testMergeFacetsCapturesStandardFacets() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().runId(sparkActionId.prevRunId).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .name("job1")
                    .namespace("namespace1")
                    .facets(
                        openLineage
                            .newJobFacetsBuilder()
                            .documentation(openLineage.newDocumentationJobFacet("doc1"))
                            .build())
                    .build())
            .build());
    prevEvents.add(openLineage.newRunEventBuilder().build());

    Map<String, JobFacet> facets = helper.prevMergedFacets();
    assertThat(facets).containsKey("documentation");
    assertThat(facets.get("documentation").getAdditionalProperties().get("description"))
        .isEqualTo("doc1");
  }

  @Test
  @SneakyThrows
  void testMergeFacetsAdditionalProperties() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));
    JobFacet jobFacet1 = openLineage.newJobFacet();
    jobFacet1.getAdditionalProperties().put("key1", "value1");
    JobFacet jobFacet2 = openLineage.newJobFacet();
    jobFacet2.getAdditionalProperties().put("key2", "value2");

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().runId(sparkActionId.prevRunId).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .name("job1")
                    .namespace("namespace1")
                    .facets(openLineage.newJobFacetsBuilder().put("custom1", jobFacet1).build())
                    .build())
            .build());
    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().runId(sparkActionId.prevRunId).build())
            .job(
                openLineage
                    .newJobBuilder()
                    .name("job1")
                    .namespace("namespace1")
                    .facets(openLineage.newJobFacetsBuilder().put("custom2", jobFacet2).build())
                    .build())
            .build());

    Map<String, JobFacet> facets = helper.prevMergedFacets();
    assertThat(facets).containsKey("custom1");
    assertThat(facets).containsKey("custom2");
    assertThat(facets.get("custom1").getAdditionalProperties().get("key1")).isEqualTo("value1");
    assertThat(facets.get("custom2").getAdditionalProperties().get("key2")).isEqualTo("value2");
  }

  @Test
  @SneakyThrows
  void testOtherJobFacetsAreSkipped() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().build())
            .job(
                openLineage
                    .newJobBuilder()
                    .name("job1")
                    .namespace("namespace1")
                    .facets(
                        openLineage
                            .newJobFacetsBuilder()
                            .documentation(openLineage.newDocumentationJobFacet("doc1"))
                            .build())
                    .build())
            .build());
    assertThat(helper.prevMergedFacets()).isEmpty();
  }
}
