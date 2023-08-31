/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.source.bytehttpkafka;

import com.linecorp.armeria.server.ServiceRequestContext;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PluginSetting;
import org.opensearch.dataprepper.model.plugin.PluginFactory;
import org.opensearch.dataprepper.model.record.Record;
import com.linecorp.armeria.common.AggregatedHttpRequest;
import com.linecorp.armeria.common.HttpData;
import com.linecorp.armeria.common.HttpResponse;
import com.linecorp.armeria.common.HttpStatus;
import com.linecorp.armeria.server.annotation.Blocking;
import com.linecorp.armeria.server.annotation.Post;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.DistributionSummary;
import io.micrometer.core.instrument.Timer;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSinkConfig;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaSinkProducer;
import org.opensearch.dataprepper.plugins.kafka.producer.KafkaSinkProducerFactory;
import org.opensearch.dataprepper.plugins.source.loghttp.codec.JsonCodec;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;


/*
* A HTTP service for log ingestion to be executed by BlockingTaskExecutor.
*/
@Blocking
public class LogHTTPService {
    public static final String REQUESTS_RECEIVED = "requestsReceived";
    public static final String SUCCESS_REQUESTS = "successRequests";
    public static final String PAYLOAD_SIZE = "payloadSize";
    public static final String REQUEST_PROCESS_DURATION = "requestProcessDuration";

    private static final Logger LOG = LoggerFactory.getLogger(LogHTTPService.class);

    // TODO: support other data-types as request body, e.g. json_lines, msgpack
    private final JsonCodec jsonCodec = new JsonCodec();
    private final Buffer<Record<byte[]>> buffer;
    private final int bufferWriteTimeoutInMillis;
    private final Counter requestsReceivedCounter;
    private final Counter successRequestsCounter;
    private final DistributionSummary payloadSizeSummary;
    private final Timer requestProcessDuration;
    private final KafkaSinkProducer producer;

    public LogHTTPService(final int bufferWriteTimeoutInMillis,
                          final Buffer<Record<byte[]>> buffer,
                          final PluginMetrics pluginMetrics,
                          final PluginSetting pluginSetting,
                          final PluginFactory pluginFactory,
                          final KafkaSinkConfig kafkaSinkConfig) {
        this.buffer = buffer;
        this.bufferWriteTimeoutInMillis = bufferWriteTimeoutInMillis;

        requestsReceivedCounter = pluginMetrics.counter(REQUESTS_RECEIVED);
        successRequestsCounter = pluginMetrics.counter(SUCCESS_REQUESTS);
        payloadSizeSummary = pluginMetrics.summary(PAYLOAD_SIZE);
        requestProcessDuration = pluginMetrics.timer(REQUEST_PROCESS_DURATION);

        final KafkaSinkProducerFactory kafkaSinkProducerFactory = new KafkaSinkProducerFactory();
        this.producer = kafkaSinkProducerFactory.createProducer(kafkaSinkConfig, pluginFactory, pluginSetting, null, null);
    }

    @Post
    public HttpResponse doPost(final ServiceRequestContext serviceRequestContext, final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        requestsReceivedCounter.increment();
        payloadSizeSummary.record(aggregatedHttpRequest.content().length());

        if(serviceRequestContext.isTimedOut()) {
            return HttpResponse.of(HttpStatus.REQUEST_TIMEOUT);
        }

        return requestProcessDuration.recordCallable(() -> processRequest(aggregatedHttpRequest));
    }

    private HttpResponse processRequest(final AggregatedHttpRequest aggregatedHttpRequest) throws Exception {
        final HttpData content = aggregatedHttpRequest.content();
        final Record<byte[]> record = new Record<>(content.array());

        try {
            producer.produceByteRecord(record).get(bufferWriteTimeoutInMillis, TimeUnit.MILLISECONDS);
        } catch (Exception e) {
            LOG.error("Failed to write the request of size {} due to: {}", content.length(), e.getMessage());
            throw e;
        }
        successRequestsCounter.increment();
        return HttpResponse.of(HttpStatus.OK);
    }
}
