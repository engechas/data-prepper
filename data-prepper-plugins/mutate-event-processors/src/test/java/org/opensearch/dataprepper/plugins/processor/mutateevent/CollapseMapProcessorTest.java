/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.processor.mutateevent;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.event.JacksonEvent;
import org.opensearch.dataprepper.model.record.Record;

import java.util.Collections;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class CollapseMapProcessorTest {
    private static final String COLLAPSED_KEY = "nvp";
    private static final String COLLAPSED_VALUE_FORMAT = "[${name}]:[${value}]";
    private static final String SOURCE = "mylist";
    private static final String TARGET = "mycollapsed";
    private static final List<Map<String, String>> ENTRIES = List.of(
            Map.of("name", "a", "value", "val-a"),
            Map.of("name", "b", "value", "val-b1"),
            Map.of("name", "b", "value", "val-b2"),
            Map.of("name", "c", "value", "val-c"));

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private CollapseMapProcessorConfig mockConfig;

    private CollapseMapProcessor processor;

    @BeforeEach
    public void setup() {
        lenient().when(mockConfig.getSource()).thenReturn(SOURCE);
        lenient().when(mockConfig.getTarget()).thenReturn(TARGET);
        lenient().when(mockConfig.getCollapsedKey()).thenReturn(COLLAPSED_KEY);
        lenient().when(mockConfig.getCollapsedValuePattern()).thenReturn(COLLAPSED_VALUE_FORMAT);
        processor = createObjectUnderTest();
    }

    @Test
    public void testFailureDueToInvalidSourceKey() {
        when(mockConfig.getSource()).thenReturn("invalid_source_key");

        final Record<Event> testRecord = createTestRecord(ENTRIES);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.containsKey("invalid_source_key"), equalTo(false));
        assertThat(resultEvent.getList(SOURCE, (Class<Map<String,String>>)(Class)Map.class), equalTo(ENTRIES));
        assertThat(resultEvent.get(TARGET, Object.class), nullValue());
    }

    @Test
    public void testCollapse() {
        final Record<Event> testRecord = createTestRecord(ENTRIES);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));
        final Event resultEvent = resultRecord.get(0).getData();

        assertEvent(resultEvent, ENTRIES, false);
    }

    @Test
    public void testSkipEntryDueToBadEventData() {
        final Record<Event> testRecord = createBadTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        final List<Map<String, String>> collapsedList = resultEvent.getList(TARGET, (Class<Map<String,String>>)(Class)Map.class);

        assertThat(collapsedList.size(), equalTo(2));
        assertThat(collapsedList.get(0), equalTo(Map.of("nvp", "[a]:[val-a]")));
        assertThat(collapsedList.get(1), equalTo(Map.of("nvp", "[c]:[val-c]")));
    }

    @Test
    public void testCollapseNotList() {
        when(mockConfig.getSource()).thenReturn("nolist");

        final Record<Event> testRecord = createTestRecord(ENTRIES);
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        final Map<String, String> collapsedMap = resultEvent.get(TARGET, (Class<Map<String,String>>)(Class)Map.class);

        assertThat(collapsedMap, equalTo(Map.of("nvp", "[d]:[val-d]")));
    }

    @Test
    public void testCollapseNotList_BadEntry() {
        when(mockConfig.getSource()).thenReturn("nolist");

        final Record<Event> testRecord = createBadTestRecord();
        final List<Record<Event>> resultRecord = (List<Record<Event>>) processor.doExecute(Collections.singletonList(testRecord));

        assertThat(resultRecord.size(), is(1));

        final Event resultEvent = resultRecord.get(0).getData();
        assertThat(resultEvent.get(TARGET, Object.class), nullValue());
    }

    private CollapseMapProcessor createObjectUnderTest() {
        return new CollapseMapProcessor(pluginMetrics, mockConfig);
    }

    private Record<Event> createTestRecord(final List<Map<String, String>> entries) {
        final Map<String, Object> data = Map.of("mylist", entries,
                "nolist", Map.of("name", "d", "value", "val-d"));
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private Record<Event> createBadTestRecord() {
        final Map<String, Object> data = Map.of("mylist", List.of(
                        Map.of("name", "a", "value", "val-a"),
                        Map.of("badname", "b", "value", "val-b"),
                        Map.of("name", "c", "value", "val-c")),
                "nolist", Map.of("badname", "d", "value", "val-d"));
        final Event event = JacksonEvent.builder()
                .withData(data)
                .withEventType("event")
                .build();
        return new Record<>(event);
    }

    private void assertEvent(final Event event, final List<Map<String, String>> originalMapEntries, final boolean removeOriginal) {
        final List<Map<String, String>> collapsedList = event.getList(TARGET, (Class<Map<String,String>>)(Class)Map.class);

        assertThat(collapsedList.size(), equalTo(originalMapEntries.size()));
        collapsedList.stream().forEach(map -> {
            assertThat(map.size(), equalTo(1));
            assertThat(map.containsKey(COLLAPSED_KEY), equalTo(true));

            final String mapValue = map.get(COLLAPSED_KEY);
            final boolean mapValueMatches = originalMapEntries.stream()
                    .map(originalMap -> Map.of(originalMap.get("name"), originalMap.get("value")))
                    .flatMap(currentMap -> currentMap.entrySet().stream())
                    .map(mapEntry -> "[" + mapEntry.getKey() + "]:[" + mapEntry.getValue() + "]")
                    .anyMatch(targetValue -> targetValue.equals(mapValue));
            assertThat(mapValueMatches, equalTo(true));
        });
    }
}
