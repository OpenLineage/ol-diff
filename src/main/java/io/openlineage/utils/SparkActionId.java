package io.openlineage.utils;

import java.util.UUID;
import lombok.Builder;
import lombok.Getter;
import lombok.Setter;

@Builder
@Setter
@Getter
public class SparkActionId {
  String jobName;
  UUID prevRunId;
  UUID nextRunId;
}
