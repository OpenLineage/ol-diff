package io.openlineage.utils;

import static org.assertj.core.api.Assertions.assertThat;

import io.openlineage.client.OpenLineage.RunEvent.EventType;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

@DisplayName("Verify Spark actions")
public class SparkActionsCase {

  private static Context context;

  @BeforeAll
  static void setup() {
    context = Context.loadContext();
  }

  @Test
  @DisplayName("Check if spark actions are the same")
  void checkSparkActions() {
    List<String> prevActions =
        context.getPrevEvents().stream()
            .filter(e -> EventType.START.equals(e.getEventType())) // get start event
            .map(e -> e.getJob().getName())
            .collect(Collectors.toList());

    List<String> nextActions =
        context.getPrevEvents().stream()
            .filter(e -> EventType.START.equals(e.getEventType())) // get start event
            .map(e -> e.getJob().getName())
            .collect(Collectors.toList());

    assertThat(prevActions)
        .describedAs("Job names corresponding to the spark actions in the previous run")
        .isEqualTo(nextActions);
  }
}
