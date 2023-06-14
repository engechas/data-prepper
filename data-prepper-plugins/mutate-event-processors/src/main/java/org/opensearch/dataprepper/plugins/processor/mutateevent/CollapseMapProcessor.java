package org.opensearch.dataprepper.plugins.processor.mutateevent;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.processor.AbstractProcessor;
import org.opensearch.dataprepper.model.processor.Processor;
import org.opensearch.dataprepper.model.record.Record;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import static org.opensearch.dataprepper.logging.DataPrepperMarkers.EVENT;

@DataPrepperPlugin(name = "collapse_list", pluginType = Processor.class, pluginConfigurationType = CollapseMapProcessorConfig.class)
public class CollapseMapProcessor extends AbstractProcessor<Record<Event>, Record<Event>> {
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
    private static final Logger LOG = LoggerFactory.getLogger(CollapseMapProcessor.class);
    private final CollapseMapProcessorConfig config;

    @DataPrepperPluginConstructor
    public CollapseMapProcessor(final PluginMetrics pluginMetrics, final CollapseMapProcessorConfig config) {
        super(pluginMetrics);
        this.config = config;
    }

    @Override
    public Collection<Record<Event>> doExecute(final Collection<Record<Event>> records) {
        for (final Record<Event> record : records) {
            final Event recordEvent = record.getData();

            final JsonNode sourceNode;
            try {
                sourceNode = getSourceNode(recordEvent);
            } catch (final Exception e) {
                LOG.warn(EVENT, "Given source path [{}] is not valid on record [{}]",
                        config.getSource(), recordEvent, e);
                continue;
            }

            final Object targetValue;
            try {
                targetValue = constructTargetValue(sourceNode, recordEvent);
            } catch (final Exception e) {
                LOG.error(EVENT, "Error collapsing map on record [{}]", recordEvent, e);
                continue;
            }

            try {
                recordEvent.put(config.getTarget(), targetValue);
            } catch (final Exception e) {
                LOG.error(EVENT, "Error updating record [{}] after collapsing map", recordEvent, e);
            }
        }
        return records;
    }

    private JsonNode getSourceNode(final Event recordEvent) {
        final Object sourceObject = recordEvent.get(config.getSource(), Object.class);
        return OBJECT_MAPPER.convertValue(sourceObject, JsonNode.class);
    }

    private Object constructTargetValue(final JsonNode sourceNode, final Event recordEvent) {
        if (sourceNode.isArray()) {
            return buildArrayTargetValue(sourceNode, recordEvent);
        }

        return buildTargetValue(recordEvent);
    }

    private List<Map<String, String>> buildArrayTargetValue(final JsonNode sourceNode, final Event recordEvent) {
        final List<Map<String, String>> targetValue = new ArrayList<>();
        for (int i = 0; i < sourceNode.size(); i++) {
            final String fullPath = config.getSource() + "/" + i + "/";
            final String collapsedValue = getCollapsedValue(fullPath, recordEvent);

            if (Objects.isNull(collapsedValue)) {
                LOG.warn("Did not find entries for one or more pointers specified by [{}] for an entry in the list of maps at [{}] on record [{}]",
                        config.getCollapsedValuePattern(), config.getSource(), recordEvent);
                continue;
            }

            targetValue.add(Map.of(config.getCollapsedKey(), collapsedValue));
        }

        return targetValue;
    }

    private Map<String, String> buildTargetValue(final Event recordEvent) {
        final String fullPath = config.getSource() + "/";
        final String collapsedValue = getCollapsedValue(fullPath, recordEvent);

        if (Objects.isNull(collapsedValue)) {
            LOG.warn("Did not find entries for one or more pointers specified by [{}] at [{}] on record [{}]",
                    config.getCollapsedValuePattern(), config.getSource(), recordEvent);
            return null;
        }

        return Map.of(config.getCollapsedKey(), collapsedValue);
    }

    private String getCollapsedValue(final String fullPath, final Event recordEvent){
        final String formatString = config.getCollapsedValuePattern().replace("${", "${" + fullPath);

        return recordEvent.formatString(formatString);
    }

    @Override
    public void prepareForShutdown() {
    }

    @Override
    public boolean isReadyForShutdown() {
        return true;
    }

    @Override
    public void shutdown() {
    }
}
