/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.buffer.noopbuffer;

import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.AbstractBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.record.Record;

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;

@DataPrepperPlugin(name = "noop_buffer", pluginType = Buffer.class)
public class NoOpBuffer<T extends Record<?>> extends AbstractBuffer<T> {
    @DataPrepperPluginConstructor
    public NoOpBuffer(final PluginSetting pluginSetting) {
        super(pluginSetting);
    }

    @Override
    public void doWrite(T record, int timeoutInMillis) {
    }

    @Override
    public void doWriteAll(Collection<T> records, int timeoutInMillis) {
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> doRead(int timeoutInMillis) {
        final List<T> records = new ArrayList<>();

        final CheckpointState checkpointState = new CheckpointState(0);
        return new AbstractMap.SimpleEntry<>(records, checkpointState);
    }

    @Override
    public void doCheckpoint(final CheckpointState checkpointState) {
    }

    @Override
    public boolean isEmpty() {
        return true;
    }
}
