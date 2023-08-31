/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.buffer;

import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.CheckpointState;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.AbstractBuffer;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.plugins.buffer.blockingbuffer.BlockingBuffer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaSinkProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaSinkProducerFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

@DataPrepperPlugin(name = "kafka_buffer", pluginType = Buffer.class, pluginConfigurationType = KafkaSinkConfig.class)
public class KafkaBuffer<T extends Record<?>> extends AbstractBuffer<T> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaBuffer.class);
    private static final int BUFFER_SIZE = 1000000;
    private static final int BATCH_SIZE = 250000;
    private final AbstractBuffer innerBuffer;
    private final KafkaSinkProducer producer;

    private final ExecutorService executorService;


    @DataPrepperPluginConstructor
    public KafkaBuffer(final PluginSetting pluginSetting, final KafkaSinkConfig kafkaSinkConfig, final PluginFactory pluginFactory,
                       final AcknowledgementSetManager acknowledgementSetManager, final PluginMetrics pluginMetrics) {
        super(pluginSetting);
        this.innerBuffer = new BlockingBuffer<>(BUFFER_SIZE, BATCH_SIZE, pluginSetting.getPipelineName());

        final KafkaSinkProducerFactory kafkaSinkProducerFactory = new KafkaSinkProducerFactory();
        this.producer = kafkaSinkProducerFactory.createProducer(kafkaSinkConfig, pluginFactory, pluginSetting, null, null);

        final KafkaSourceConfig kafkaSourceConfig = convertSinkConfigToSourceConfig(kafkaSinkConfig);
        final KafkaSourceCustomConsumerFactory kafkaSourceCustomConsumerFactory = new KafkaSourceCustomConsumerFactory();
        final List<KafkaSourceCustomConsumer> consumers = kafkaSourceCustomConsumerFactory.createConsumersForTopic(kafkaSourceConfig, kafkaSinkConfig.getTopic(),
                innerBuffer, pluginMetrics, acknowledgementSetManager, new AtomicBoolean(false));

        this.executorService = Executors.newFixedThreadPool(consumers.size());
        consumers.forEach(this.executorService::submit);
    }

    @Override
    public void doWrite(T record, int timeoutInMillis) {
        try {
            producer.produceByteRecord(record).get(timeoutInMillis, TimeUnit.MILLISECONDS);
        } catch (final Exception e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void doWriteAll(Collection<T> records, int timeoutInMillis) {
    }

    @Override
    public Map.Entry<Collection<T>, CheckpointState> doRead(int timeoutInMillis) {
        return innerBuffer.doRead(timeoutInMillis);
    }

    @Override
    public void postProcess(final Long recordsInBuffer) {
        innerBuffer.postProcess(recordsInBuffer);
    }

    @Override
    public void doCheckpoint(final CheckpointState checkpointState) {
        innerBuffer.doCheckpoint(checkpointState);
    }

    @Override
    public boolean isEmpty() {
        return innerBuffer.isEmpty();
    }

    private KafkaSourceConfig convertSinkConfigToSourceConfig(final KafkaSinkConfig kafkaSinkConfig) {
        final KafkaSourceConfig kafkaSourceConfig = new KafkaSourceConfig();
        kafkaSourceConfig.setAuthConfig(kafkaSinkConfig.getAuthConfig());
        kafkaSourceConfig.setAwsConfig(kafkaSinkConfig.getAwsConfig());
        kafkaSourceConfig.setTopics(List.of(kafkaSinkConfig.getTopic()));
        kafkaSourceConfig.setAcknowledgementsEnabled(kafkaSinkConfig.getAcknowledgments());

        return kafkaSourceConfig;
    }
}
