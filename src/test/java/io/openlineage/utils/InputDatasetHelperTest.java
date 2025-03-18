package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.InputDataset;
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
public class InputDatasetHelperTest {

  List<RunEvent> prevEvents = new ArrayList<>();
  List<RunEvent> nextEvents = new ArrayList<>();
  Context context = new Context(prevEvents, nextEvents, new Config());
  SparkActionId sparkActionId = new SparkActionId("spark", UUID.randomUUID(), UUID.randomUUID());
  InputDatasetHelper helper = new InputDatasetHelper(context, sparkActionId);

  @BeforeEach
  @SneakyThrows
  void setup() {
    OpenLineage openLineage = new OpenLineage(new URI("http://localhost:5000"));

    InputDataset input1 =
        openLineage.newInputDataset(
            "namespace",
            "i1",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(openLineage.newSymlinksDatasetFacetBuilder().build())
                .put("inputFacet1", openLineage.newDatasetFacet())
                .build(),
            openLineage.newInputDatasetInputFacetsBuilder().build());

    InputDataset input2 =
        openLineage.newInputDataset(
            "namespace",
            "i2",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(openLineage.newSymlinksDatasetFacetBuilder().build())
                .put("inputFacet2", openLineage.newDatasetFacet())
                .build(),
            openLineage
                .newInputDatasetInputFacetsBuilder()
                .inputStatistics(openLineage.newInputStatisticsInputDatasetFacetBuilder().build())
                .put("inputFacet3", openLineage.newInputDatasetFacet())
                .build());

    InputDataset input3 =
        openLineage.newInputDataset(
            "namespace",
            "i3",
            openLineage.newDatasetFacetsBuilder().build(),
            openLineage.newInputDatasetInputFacetsBuilder().build());

    prevEvents.add(
        openLineage
            .newRunEventBuilder()
            .run(openLineage.newRunBuilder().runId(sparkActionId.prevRunId).build())
            .inputs(new ArrayList<>(List.of(input1, input2, input3)))
            .build());

    prevEvents.add(
        openLineage.newRunEventBuilder().inputs(Collections.singletonList(input1)).build());
  }

  @Test
  void testPrevDatasetFacets() {
    assertThat(helper.prevFacets())
        .hasSize(2)
        .containsKey(new DatasetIdentifier("i1", "namespace"))
        .containsKey(new DatasetIdentifier("i2", "namespace"));

    assertThat(helper.prevFacets().get(new DatasetIdentifier("i1", "namespace")))
        .containsExactlyInAnyOrder("symlinks", "inputFacet1");

    assertThat(helper.prevFacets().get(new DatasetIdentifier("i2", "namespace")))
        .containsExactlyInAnyOrder("symlinks", "inputFacet2");
  }

  @Test
  void testPrevInputDatasetFacets() {
    assertThat(helper.prevInputFacets())
        .hasSize(1)
        .containsKey(new DatasetIdentifier("i2", "namespace"));

    assertThat(helper.prevInputFacets().get(new DatasetIdentifier("i2", "namespace")))
        .containsExactlyInAnyOrder("inputStatistics", "inputFacet3");
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
