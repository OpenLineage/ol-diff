/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunFacet;
import java.net.URI;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("internal-test")
public class RunHelperTest {

  List<RunEvent> prevEvents = new ArrayList<>();
  List<RunEvent> nextEvents = new ArrayList<>();
  Context context = new Context(prevEvents, nextEvents, new Config());
  SparkActionId sparkActionId = new SparkActionId("spark", UUID.randomUUID(), UUID.randomUUID());
  RunHelper helper = new RunHelper(context, sparkActionId);

  @Test
  @SneakyThrows
  void testMergeFacetsCapturesStandardFacets() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(sparkActionId.prevRunId)
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .errorMessage(
                                openLineage.newErrorMessageRunFacet("error1", "error2", "error3"))
                            .build())
                    .build())
            .build());
    prevEvents.add(openLineage.newRunEventBuilder().build());
    Map<String, RunFacet> facets = helper.prevMergedFacets();
    assertThat(facets).containsKey("errorMessage");
    assertThat(facets.get("errorMessage").getAdditionalProperties().get("message"))
        .isEqualTo("error1");
  }

  @Test
  @SneakyThrows
  void testMergeFacetsAdditionalProperties() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));
    RunFacet runFacet1 = openLineage.newRunFacet();
    runFacet1.getAdditionalProperties().put("key1", "value1");
    RunFacet runFacet2 = openLineage.newRunFacet();
    runFacet2.getAdditionalProperties().put("key2", "value2");

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(sparkActionId.prevRunId)
                    .facets(openLineage.newRunFacetsBuilder().put("custom1", runFacet1).build())
                    .build())
            .build());
    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(sparkActionId.prevRunId)
                    .facets(openLineage.newRunFacetsBuilder().put("custom2", runFacet2).build())
                    .build())
            .build());

    Map<String, RunFacet> facets = helper.prevMergedFacets();
    assertThat(facets).containsKey("custom1");
    assertThat(facets).containsKey("custom2");
    assertThat(facets.get("custom1").getAdditionalProperties().get("key1")).isEqualTo("value1");
    assertThat(facets.get("custom2").getAdditionalProperties().get("key2")).isEqualTo("value2");
  }

  @Test
  @SneakyThrows
  void testOtherRunFacetsAreSkipped() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(
                openLineage
                    .newRunBuilder()
                    .runId(UUID.randomUUID())
                    .facets(
                        openLineage
                            .newRunFacetsBuilder()
                            .errorMessage(
                                openLineage.newErrorMessageRunFacet("error1", "error2", "error3"))
                            .build())
                    .build())
            .build());
    prevEvents.add(openLineage.newRunEventBuilder().build());

    assertThat(helper.prevMergedFacets()).isEmpty();
  }
}
