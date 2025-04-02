package io.openlineage.utils;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacet;
import io.openlineage.client.OpenLineage.SymlinksDatasetFacetIdentifiers;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public class DatasetUtils {

  /**
   * Checks if two datasets have the same name. Datasets are the same if they have the same dataset
   * identifier or any of their symlinks are the same.
   */
  public static boolean areSameName(Dataset o1, Dataset o2) {
    Set<DatasetName> o1Names = namesFrom(o1);
    Set<DatasetName> o2Names = namesFrom(o2);

    Set<DatasetName> intersection = o1Names;
    intersection.retainAll(o2Names);
    return !intersection.isEmpty();
  }

  private static Set<DatasetName> namesFrom(Dataset dataset) {
    Set<DatasetName> names = new HashSet<>();
    names.add(new DatasetName(dataset.getNamespace(), dataset.getName()));

    if (dataset.getFacets() != null && dataset.getFacets().getSymlinks() != null) {
      SymlinksDatasetFacet symlinksFacet = dataset.getFacets().getSymlinks();
      if (symlinksFacet.getIdentifiers() != null) {
        for (SymlinksDatasetFacetIdentifiers identifier : symlinksFacet.getIdentifiers()) {
          names.add(new DatasetName(identifier.getNamespace(), identifier.getName()));
        }
      }
    }

    return names;
  }

  @EqualsAndHashCode
  @AllArgsConstructor
  private static class DatasetName {
    String namespace;
    String name;
  }
}
