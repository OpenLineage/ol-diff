/*
/* Copyright 2018-2025 contributors to the OpenLineage project
/* SPDX-License-Identifier: Apache-2.0
*/

package io.openlineage.utils;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import io.openlineage.client.OpenLineage.RunEvent;
import io.openlineage.client.OpenLineage.RunEvent.EventType;
import io.openlineage.client.OpenLineageClientUtils;
import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;

/** Common methods to be shared among the tests */
@Getter
@AllArgsConstructor
@Slf4j
public class Context {

  private static final String CONSOLE_TRANSPORT_LOG = "ConsoleTransport: ";

  List<RunEvent> prevEvents;
  List<RunEvent> nextEvents;
  Config config;

  public static Context loadContext() {
    Config config;
    if (!System.getProperty("configYaml").isEmpty()) {
      ObjectMapper objectMapper = new ObjectMapper(new YAMLFactory());

      log.info("Loading config from {}", System.getProperty("configYaml"));
      try {
        config = objectMapper.readValue(new File(System.getProperty("configYaml")), Config.class);

        log.info("Config loaded: {}", OpenLineageClientUtils.toJson(config));
      } catch (Exception e) {
        log.error("Error loading config from {}", System.getProperty("configYaml"), e);
        config = new Config();
      }
    } else {
      log.info("No config yaml provided, using default config");
      config = new Config();
    }

    return new Context(
        getRunEvents(System.getProperty("prev.path")),
        getRunEvents(System.getProperty("next.path")),
        config);
  }

  @SneakyThrows
  public static List<RunEvent> getRunEvents(String path) {
    boolean containsJsons= Files
        .lines(Path.of(path))
        .filter(line -> line.trim().startsWith("{") && !line.contains(CONSOLE_TRANSPORT_LOG))
        .findFirst()
        .isPresent();

    if (containsJsons) {
      log.info("Loading run events from jsons");
      return getRunEventsFromJsons(path);
    } else {
      log.info("Loading run events from logs");
      return getRunEventsFromLogs(path);
    }
  }

  @SneakyThrows
  public static List<RunEvent> getRunEventsFromLogs(String path) {
    List<String> lines = Files.lines(Path.of(path)).collect(Collectors.toList());
    List<RunEvent> events = new ArrayList<>();

    // newlines can also occur in the logs in the middle of OpenLineage event
    boolean inEvent = false;
    StringBuilder json = new StringBuilder();
    for (String line : lines) {
      boolean isLogLine = line.matches("\\d\\d/\\d\\d/\\d\\d.*");
      if (line.contains(CONSOLE_TRANSPORT_LOG)) {
        log.info("Starting new event");
        inEvent = true;
        json = new StringBuilder();
        int index = line.indexOf(CONSOLE_TRANSPORT_LOG);
        json.append(line.substring(index + CONSOLE_TRANSPORT_LOG.length()));
      } else {
        if (inEvent) {
          if (isLogLine) {
            // ending the event
            log.info("Whole event detected: {}", json.toString());
            events.add(OpenLineageClientUtils.runEventFromJson(json.toString()));
            inEvent = false;

            // see if the new linea is not starting a new event
            if (line.contains(CONSOLE_TRANSPORT_LOG)) {
              log.info("Starting new event");
              inEvent = true;
              json = new StringBuilder();
              int index = line.indexOf(CONSOLE_TRANSPORT_LOG);
              json.append(line.substring(index + CONSOLE_TRANSPORT_LOG.length()));
            }
          } else {
            // append whole line to json
            log.info("Appending new json line");
            json.append(line);
          }
        }
      }
    }
    return events;
  }

  @SneakyThrows
  private static List<RunEvent> getRunEventsFromJsons(String path) {
    return Collections.emptyList();
  }

  public List<SparkActionId> getSparkActionsIds() {
    List<UUID> prevUuids =
        prevEvents.stream()
            .filter(e -> EventType.START.equals(e.getEventType())) // get start event
            .map(e -> e.getRun().getRunId())
            .collect(Collectors.toList());

    List<String> prevJobs =
        prevEvents.stream()
            .filter(e -> EventType.START.equals(e.getEventType())) // get start event
            .map(e -> e.getJob().getName())
            .collect(Collectors.toList());

    List<UUID> nextUuids =
        nextEvents.stream()
            .filter(e -> EventType.START.equals(e.getEventType())) // get start event
            .map(e -> e.getRun().getRunId())
            .collect(Collectors.toList());

    if (prevUuids.size() != nextUuids.size()) {
      log.warn("Different number of spark actions in the previous and next run");
      return Collections.emptyList();
    }

    List<SparkActionId> actionIds = new ArrayList<>();
    for (int i = 0; i < prevUuids.size(); i++) {
      actionIds.add(
          SparkActionId.builder()
              .jobName(prevJobs.get(i))
              .nextRunId(nextUuids.get(i))
              .prevRunId(prevUuids.get(i))
              .build());
    }
    if (actionIds.isEmpty()) {
      log.warn("No spark actions found in the previous and next run");
    }
    return actionIds;
  }
}
