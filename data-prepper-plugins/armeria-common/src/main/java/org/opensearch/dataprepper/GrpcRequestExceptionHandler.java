/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper;

import com.linecorp.armeria.common.RequestContext;
import com.linecorp.armeria.common.annotation.Nullable;
import com.linecorp.armeria.common.grpc.GrpcStatusFunction;
import com.linecorp.armeria.server.RequestTimeoutException;
import io.grpc.Metadata;
import io.grpc.Status;
import io.micrometer.core.instrument.Counter;
import org.opensearch.dataprepper.exceptions.BadRequestException;
import org.opensearch.dataprepper.exceptions.BufferWriteException;
import org.opensearch.dataprepper.exceptions.RequestCancelledException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.buffer.SizeOverflowException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeoutException;

public class GrpcRequestExceptionHandler implements GrpcStatusFunction {
    private static final Logger LOG = LoggerFactory.getLogger(GrpcRequestExceptionHandler.class);
    public static final String REQUEST_TIMEOUTS = "requestTimeouts";
    public static final String BAD_REQUESTS = "badRequests";
    public static final String REQUESTS_TOO_LARGE = "requestsTooLarge";
    public static final String INTERNAL_SERVER_ERROR = "internalServerError";

    private final Counter requestTimeoutsCounter;
    private final Counter badRequestsCounter;
    private final Counter requestsTooLargeCounter;
    private final Counter internalServerErrorCounter;

    public GrpcRequestExceptionHandler(final PluginMetrics pluginMetrics) {
        requestTimeoutsCounter = pluginMetrics.counter(REQUEST_TIMEOUTS);
        badRequestsCounter = pluginMetrics.counter(BAD_REQUESTS);
        requestsTooLargeCounter = pluginMetrics.counter(REQUESTS_TOO_LARGE);
        internalServerErrorCounter = pluginMetrics.counter(INTERNAL_SERVER_ERROR);
    }

    @Override
    public @Nullable Status apply(final RequestContext context, final Throwable exception, final Metadata metadata) {
        final Throwable exceptionCause = exception instanceof BufferWriteException ? exception.getCause() : exception;

        return handleExceptions(exceptionCause);
    }

    private Status handleExceptions(final Throwable e) {
        if (e instanceof RequestTimeoutException || e instanceof TimeoutException) {
            requestTimeoutsCounter.increment();
            return createStatus(e, Status.RESOURCE_EXHAUSTED);
        } else if (e instanceof SizeOverflowException) {
            requestsTooLargeCounter.increment();
            return createStatus(e, Status.RESOURCE_EXHAUSTED);
        } else if (e instanceof BadRequestException) {
            badRequestsCounter.increment();
            return createStatus(e, Status.INVALID_ARGUMENT);
        } else if (e instanceof RequestCancelledException) {
            requestTimeoutsCounter.increment();
            return createStatus(e, Status.CANCELLED);
        }

        internalServerErrorCounter.increment();
        return createStatus(e, Status.INTERNAL);
    }

    private Status createStatus(final Throwable e, final Status status) {
        return status.withDescription(e.getMessage());
    }
}
