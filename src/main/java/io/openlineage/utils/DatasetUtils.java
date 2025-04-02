package io.openlineage.utils;

import io.openlineage.client.OpenLineage.Dataset;
import io.openlineage.client.utils.DatasetIdentifier;
import io.openlineage.client.utils.DatasetIdentifier.Symlink;
import io.openlineage.client.utils.DatasetIdentifier.SymlinkType;
import java.util.HashSet;
import java.util.Set;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;

public class DatasetUtils {

  public static boolean areSameName(Dataset o1, Dataset o2) {
    DatasetIdentifier o1Identifier = toDatasetIdentifier(o1);
    DatasetIdentifier o2Identifier = toDatasetIdentifier(o2);

    return areSameName(o1Identifier, o2Identifier);
  }

  /**
   * Checks if two datasets have the same name. Datasets are the same if they have the same dataset
   * identifier or any of their symlinks are the same.
   */
  public static boolean areSameName(DatasetIdentifier o1, DatasetIdentifier o2) {
    Set<DatasetName> o1Names = namesFrom(o1);
    Set<DatasetName> o2Names = namesFrom(o2);

    Set<DatasetName> intersection = o1Names;
    intersection.retainAll(o2Names);
    return !intersection.isEmpty();
  }

  public static DatasetIdentifier toDatasetIdentifier(Dataset dataset) {
    DatasetIdentifier identifier = new DatasetIdentifier(dataset.getName(), dataset.getNamespace());

    if (dataset.getFacets() != null
        && dataset.getFacets().getSymlinks() != null
        && dataset.getFacets().getSymlinks().getIdentifiers() != null) {
      dataset
          .getFacets()
          .getSymlinks()
          .getIdentifiers()
          .forEach(
              s -> {
                identifier.withSymlink(
                    s.getName(), s.getNamespace(), SymlinkType.valueOf(s.getType()));
              });
    }

    return identifier;
  }

  private static Set<DatasetName> namesFrom(DatasetIdentifier dataset) {
    Set<DatasetName> names = new HashSet<>();
    names.add(new DatasetName(dataset.getNamespace(), dataset.getName()));

    if (dataset.getSymlinks() != null) {
      for (Symlink identifier : dataset.getSymlinks()) {
        names.add(new DatasetName(identifier.getNamespace(), identifier.getName()));
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
