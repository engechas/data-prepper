package org.opensearch.dataprepper.feedback;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.inject.Named;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

@Named
public class Observer {
    private static final Logger LOG = LoggerFactory.getLogger(Observer.class);

    private static final long BULK_MODIFIER = 1024 * 1024 * 2;
    private static final int PERIODS_TO_EVALUATE = 3;
    private static final String LATENCY_METRIC = "s3-log-pipeline.opensearch.bulkRequestLatency";
    private static final String THROUGHPUT_METRIC = "s3-log-pipeline.opensearch.documentsSuccess";
    private static final Predicate<Double> IS_DATA_INVALID = data -> data == -1d;

    private static final Predicate<Double> LATENCY_TOO_LOW = latency -> latency < 1000d;
    private static final List<Predicate<Double>> LATENCY_ADJUST_UP_PREDICATES = List.of(
            LATENCY_TOO_LOW
    );

    private static final Predicate<Double> LATENCY_TOO_HIGH = latency -> latency > 10000d;
    private static final List<Predicate<Double>> LATENCY_ADJUST_DOWN_PREDICATES = List.of(
            LATENCY_TOO_HIGH
    );

    private static final Map<String, List<Predicate<Double>>> ADJUST_UP = Map.of(
            LATENCY_METRIC, LATENCY_ADJUST_UP_PREDICATES,
            THROUGHPUT_METRIC, Collections.emptyList()
    );

    private static final Map<String, List<Predicate<Double>>> ADJUST_DOWN = Map.of(
            LATENCY_METRIC, LATENCY_ADJUST_DOWN_PREDICATES,
            THROUGHPUT_METRIC, Collections.emptyList()
    );

    private static final List<String> METRICS_TO_OBSERVE = List.of(
            LATENCY_METRIC,
            THROUGHPUT_METRIC
    );
    private static final ScheduledExecutorService EXECUTOR_SERVICE = Executors.newSingleThreadScheduledExecutor();

    private final Map<String, ObserverData> observedConfigurations = new HashMap<>();
    private long currentBulkSize = -1;

    public Observer() {
        EXECUTOR_SERVICE.scheduleAtFixedRate(this::evaluateData, 5, 1, TimeUnit.MINUTES);
    }

    public void registerObserver(final Timer timer) {
        final String metricName = timer.getId().getName();
        final Runnable addDataToObeserver = () -> addDataToObserver(metricName, timer);

        registerObserver(metricName, addDataToObeserver);
    }

    public void registerObserver(final Counter counter) {
        final String metricName = counter.getId().getName();
        final Runnable addDataToObeserver = () -> addDataToObserver(metricName, counter);

        registerObserver(metricName, addDataToObeserver);
    }

    private void registerObserver(final String metricName, final Runnable addDataToObserver) {
        if (METRICS_TO_OBSERVE.contains(metricName)) {
            observedConfigurations.put(metricName, new ObserverData());
        }

        EXECUTOR_SERVICE.scheduleAtFixedRate(addDataToObserver, 1, 1, TimeUnit.MINUTES);
    }

    private void addDataToObserver(final String metricName, final Meter meter) {
        if (currentBulkSize == -1) {
            LOG.warn("No configuration value registered for bulk size. Cannot observe data.");
            return;
        }

        final ObserverData data = observedConfigurations.get(metricName);

        final double dataPoint;
        if (meter instanceof Timer) {
            dataPoint = getMeanFromTimer((Timer) meter);
        } else if (meter instanceof Counter) {
            dataPoint = getCountFromCounter((Counter) meter);
        } else {
            throw new UnsupportedOperationException("No meter type registered for metric name: " + metricName);
        }

        data.addDataPoint(currentBulkSize, dataPoint);
        LOG.warn("Metric [{}], at configuration [{}] data point was [{}]. Total data points: [{}]", metricName,
                currentBulkSize, dataPoint, data.getDataForConfiguration(currentBulkSize));
    }

    private double getMeanFromTimer(final Timer timer) {
        final HistogramSnapshot meterData = timer.takeSnapshot();
        return meterData.mean(TimeUnit.MILLISECONDS);
    }

    private double getCountFromCounter(final Counter counter) {
        return counter.count();
    }

    private void evaluateData() {
        try {
            doEvaluateData();
        } catch (final Exception e) {
            LOG.error("Caught exception evaluating data", e);
        }
    }

    private void doEvaluateData() {
        final Map<String, Double> metricsToDataPoints = getDataForMetrics();
        final boolean isDataInvalid = metricsToDataPoints.values().stream().anyMatch(IS_DATA_INVALID);
        if (isDataInvalid) {
            LOG.warn("One or more data points were invalid. Skipping evaluation.");
            return;
        }

        final boolean isAdjustDown = METRICS_TO_OBSERVE.stream()
                .anyMatch(metricName -> processAdjustDownForMetric(metricName, metricsToDataPoints.get(metricName)));

        if (isAdjustDown) {
            final Long newBulkSize = currentBulkSize - BULK_MODIFIER;
            LOG.warn("Bulk size is too high, decreasing bulk size by [{}] to [{}]", BULK_MODIFIER, newBulkSize);
            currentBulkSize = newBulkSize;
            return;
        }

        final boolean isAdjustUp = METRICS_TO_OBSERVE.stream()
                .allMatch(metricName -> processAdjustUpForMetric(metricName, metricsToDataPoints.get(metricName)));

        if (isAdjustUp) {
            final Long newBulkSize = currentBulkSize + BULK_MODIFIER;
            LOG.warn("Bulk size is too low, increasing bulk size by [{}] to [{}]", BULK_MODIFIER, newBulkSize);
            currentBulkSize = newBulkSize;
            return;
        }

        LOG.warn("No adjustment criteria met. Bulk size stays at [{}]", currentBulkSize);
    }

    private Map<String, Double> getDataForMetrics() {
        return METRICS_TO_OBSERVE.stream()
                .map(this::getDataForMetric)
                .collect(Collectors.toMap(Map.Entry::getKey, Map.Entry::getValue));
    }

    private Map.Entry<String, Double> getDataForMetric(final String metricName) {
        final ObserverData data = observedConfigurations.get(metricName);
        final List<Double> rawValues = data.getDataForConfiguration(currentBulkSize);
        final List<Double> recentData = rawValues.stream()
                .skip(Math.max(0, rawValues.size() - PERIODS_TO_EVALUATE))
                .filter(value -> value > 0d)
                .collect(Collectors.toList());

        if (recentData.size() < PERIODS_TO_EVALUATE) {
            LOG.warn("Less than {} valid values found for metric [{}], skipping evaluation.", PERIODS_TO_EVALUATE, metricName);
            return Map.entry(metricName, -1d);
        }

        final double averageValue = recentData.stream().mapToDouble(Double::doubleValue).average().orElse(-1d);
        LOG.warn("Found average for [{}] over last 5 data points [{}]", metricName, averageValue);

        if (averageValue == -1d) {
            LOG.warn("Problem calculating average for metric [{}]. Skipping evaluation.", metricName);
            return Map.entry(metricName, -1d);
        }

        return Map.entry(metricName, averageValue);
    }

    private boolean processAdjustDownForMetric(final String metricName, final double dataPoint) {
        final List<Predicate<Double>> adjustDownPredicates = ADJUST_DOWN.get(metricName);
        // If there's any conditions indicating we need to adjust down, do so
        return adjustDownPredicates.stream().anyMatch(predicate -> predicate.test(dataPoint));
    }

    private boolean processAdjustUpForMetric(final String metricName, final double dataPoint) {
        final List<Predicate<Double>> adjustUpPredicates = ADJUST_UP.get(metricName);
        // If all conditions indicating we need to adjust up, do so
        return adjustUpPredicates.stream().allMatch(predicate -> predicate.test(dataPoint));
    }

    public void registerCurrentConfigurationValue(final String metricName, final Long configurationValue) {
        LOG.warn("Metric [{}] registered current configuration [{}]", metricName, configurationValue);
        currentBulkSize = configurationValue;
    }

    public Long getConfigurationValue() {
        return currentBulkSize;
    }

    public List<String> getMetricsToObserve() {
        return METRICS_TO_OBSERVE;
    }
}
