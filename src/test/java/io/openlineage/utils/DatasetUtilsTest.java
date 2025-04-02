package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage;
import io.openlineage.client.OpenLineage.Dataset;
import java.net.URI;
import java.util.Collections;
import lombok.SneakyThrows;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("internal-test")
public class DatasetUtilsTest {

  OpenLineage openLineage;

  @BeforeEach
  @SneakyThrows
  void setup() {
    openLineage = new OpenLineage(new URI("http://localhost:5000"));
  }

  @Test
  void testDatasetIdentifier() {
    Dataset d1 = openLineage.newInputDataset("namespace", "i1", null, null);
    Dataset d2 = openLineage.newInputDataset("namespace", "i1", null, null);
    Dataset d3 = openLineage.newInputDataset("namespace", "i2", null, null);

    assertThat(DatasetUtils.areSameName(d1, d2)).isTrue();
    assertThat(DatasetUtils.areSameName(d1, d3)).isFalse();
  }

  @Test
  void testDatasetIdentifierWithSameSymlinks() {
    Dataset d1 =
        openLineage.newInputDataset(
            "namespace",
            "i1",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(
                    openLineage.newSymlinksDatasetFacet(
                        Collections.singletonList(
                            openLineage.newSymlinksDatasetFacetIdentifiers(
                                "symlink-namespace", "symlink-name", "table"))))
                .build(),
            null);
    Dataset d2 =
        openLineage.newInputDataset(
            "namespace",
            "i2",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(
                    openLineage.newSymlinksDatasetFacet(
                        Collections.singletonList(
                            openLineage.newSymlinksDatasetFacetIdentifiers(
                                "symlink-namespace", "symlink-name", "table"))))
                .build(),
            null);

    assertThat(DatasetUtils.areSameName(d1, d2)).isTrue();
  }

  @Test
  void testDatasetIdentifierWithDifferentSymlinks() {
    Dataset d1 =
        openLineage.newInputDataset(
            "namespace",
            "i1",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(
                    openLineage.newSymlinksDatasetFacet(
                        Collections.singletonList(
                            openLineage.newSymlinksDatasetFacetIdentifiers(
                                "symlink-namespace", "symlink-name1", "table"))))
                .build(),
            null);
    Dataset d2 =
        openLineage.newInputDataset(
            "namespace",
            "i2",
            openLineage
                .newDatasetFacetsBuilder()
                .symlinks(
                    openLineage.newSymlinksDatasetFacet(
                        Collections.singletonList(
                            openLineage.newSymlinksDatasetFacetIdentifiers(
                                "symlink-namespace", "symlink-name2", "table"))))
                .build(),
            null);

    assertThat(DatasetUtils.areSameName(d1, d2)).isFalse();
  }
}
