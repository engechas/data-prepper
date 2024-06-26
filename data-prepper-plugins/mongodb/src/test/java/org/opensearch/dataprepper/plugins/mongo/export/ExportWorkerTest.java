package org.opensearch.dataprepper.plugins.mongo.export;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourceCoordinator;
import org.opensearch.dataprepper.plugins.mongo.buffer.ExportRecordBufferWriter;
import org.opensearch.dataprepper.plugins.mongo.configuration.MongoDBSourceConfig;
import org.opensearch.dataprepper.plugins.mongo.coordination.partition.DataQueryPartition;

import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
public class ExportWorkerTest {
    @Mock
    private EnhancedSourceCoordinator sourceCoordinator;

    @Mock
    private Buffer<Record<Event>> buffer;

    @Mock
    private PluginMetrics pluginMetrics;

    @Mock
    private AcknowledgementSetManager acknowledgementSetManager;

    @Mock
    private MongoDBSourceConfig sourceConfig;

    @Mock
    private ExportRecordBufferWriter recordBufferWriter;

    private ExportWorker exportWorker;

    @BeforeEach
    public void setup() throws Exception {
        exportWorker = new ExportWorker(sourceCoordinator, buffer, pluginMetrics, acknowledgementSetManager, sourceConfig);
    }

    @Test
    void testEmptyDataQueryPartition() throws Exception {
        when(sourceCoordinator.acquireAvailablePartition(DataQueryPartition.PARTITION_TYPE)).thenReturn(Optional.empty());

        final ExecutorService executorService = Executors.newSingleThreadExecutor();
        executorService.submit(() -> exportWorker.run());
        Thread.sleep(100);
        executorService.shutdownNow();

        verifyNoInteractions(recordBufferWriter);
    }

}
