package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.OutputDataset;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.utils.DatasetIdentifier;
import java.net.URI;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("internal-test")
public class OutputDatasetHelperTest {

  List<RunEvent> prevEvents = new ArrayList<>();
  List<RunEvent> nextEvents = new ArrayList<>();
  Context context = new Context(prevEvents, nextEvents, new Config());
  SparkActionId sparkActionId = new SparkActionId("spark", UUID.randomUUID(), UUID.randomUUID());
  OutputDatasetHelper helper = new OutputDatasetHelper(context, sparkActionId);

  @BeforeEach
  @SneakyThrows
  void setup() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));

    OutputDataset output1 =
        openLineage.newOutputDataset(
            "namespace",
            "o1",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(openLineage.newSymlinksDatasetFacetBuilder().build())
                .put("outputFacet1", openLineage.newDatasetFacet())
                .build(),
            openLineage.newOutputDatasetOutputFacetsBuilder().build());

    OutputDataset output2 =
        openLineage.newOutputDataset(
            "namespace",
            "o2",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(openLineage.newSymlinksDatasetFacetBuilder().build())
                .put("outputFacet2", openLineage.newDatasetFacet())
                .build(),
            openLineage
                .newOutputDatasetOutputFacetsBuilder()
                .outputStatistics(
                    openLineage.newOutputStatisticsOutputDatasetFacetBuilder().build())
                .put(
                    "outputFacet3",
                    openLineage.newOutputStatisticsOutputDatasetFacetBuilder().build())
                .build());

    OutputDataset output3 =
        openLineage.newOutputDataset(
            "namespace",
            "o3",
            openLineage.newDatasetFacetsBuilder().build(),
            openLineage.newOutputDatasetOutputFacetsBuilder().build());

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().runId(sparkActionId.prevRunId).build())
            .outputs(new ArrayList<>(List.of(output1, output2, output3)))
            .build());

    prevEvents.add(
        openLineage.newRunEventBuilder().outputs(Collections.singletonList(output1)).build());
  }

  @Test
  void testPrevDatasetFacets() {
    assertThat(helper.prevFacets())
        .hasSize(2)
        .containsKey(new DatasetIdentifier("o1", "namespace"))
        .containsKey(new DatasetIdentifier("o2", "namespace"));

    assertThat(helper.prevFacets().get(new DatasetIdentifier("o1", "namespace")))
        .containsExactlyInAnyOrder("symlinks", "outputFacet1");

    assertThat(helper.prevFacets().get(new DatasetIdentifier("o2", "namespace")))
        .containsExactlyInAnyOrder("symlinks", "outputFacet2");
  }

  @Test
  void testPrevInputDatasetFacets() {
    assertThat(helper.prevOutputFacets())
        .hasSize(1)
        .containsKey(new DatasetIdentifier("o2", "namespace"));

    assertThat(helper.prevOutputFacets().get(new DatasetIdentifier("o2", "namespace")))
        .containsExactlyInAnyOrder("outputStatistics", "outputFacet3");
  }

  @Test
  @SneakyThrows
  void testOtherRunsInputDatasetFacets() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));
    prevEvents.clear();
    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().runId(sparkActionId.prevRunId).build())
            .inputs(
                Collections.singletonList(
                    openLineage.newInputDataset(
                        "namespace",
                        "i1",
                        openLineage
                            .newDatasetFacetsBuilder()
                            .symlinks(openLineage.newSymlinksDatasetFacetBuilder().build())
                            .put("inputFacet2", openLineage.newDatasetFacet())
                            .build(),
                        openLineage.newInputDatasetInputFacetsBuilder().build())))
            .build());

    assertThat(helper.prevFacets()).isEmpty();
  }
}
