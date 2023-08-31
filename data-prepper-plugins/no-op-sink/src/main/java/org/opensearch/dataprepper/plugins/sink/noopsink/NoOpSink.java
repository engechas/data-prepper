package org.opensearch.dataprepper.plugins.sink.noopsink;

import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.sink.AbstractSink;
import org.opensearch.dataprepper.model.sink.Sink;

import java.util.Collection;

@DataPrepperPlugin(name = "noop_sink", pluginType = Sink.class)
public class NoOpSink extends AbstractSink<Record<Event>> {
    @DataPrepperPluginConstructor
    public NoOpSink(final PluginSetting pluginSetting) {
        super(pluginSetting);
    }


    @Override
    public void doInitialize() {

    }

    @Override
    public void doOutput(Collection<Record<Event>> records) {

    }

    @Override
    public boolean isReady() {
        return true;
    }
}
