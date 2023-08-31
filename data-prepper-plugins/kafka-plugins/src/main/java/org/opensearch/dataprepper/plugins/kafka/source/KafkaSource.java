/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.kafka.source;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.common.errors.UnknownTopicOrPartitionException;
import org.opensearch.dataprepper.metrics.PluginMetrics;
import org.opensearch.dataprepper.model.acknowledgements.AcknowledgementSetManager;
import org.opensearch.dataprepper.model.annotations.DataPrepperPlugin;
import org.opensearch.dataprepper.model.annotations.DataPrepperPluginConstructor;
import org.opensearch.dataprepper.model.buffer.Buffer;
import org.opensearch.dataprepper.model.configuration.PipelineDescription;
import org.opensearch.dataprepper.model.event.Event;
import org.opensearch.dataprepper.model.record.Record;
import org.opensearch.dataprepper.model.source.Source;
import org.opensearch.dataprepper.plugins.kafka.configuration.KafkaSourceConfig;
import org.opensearch.dataprepper.plugins.kafka.configuration.TopicConfig;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumer;
import org.opensearch.dataprepper.plugins.kafka.consumer.KafkaSourceCustomConsumerFactory;
import org.opensearch.dataprepper.plugins.kafka.util.MessageFormat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.InetAddress;
import java.net.Socket;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.URL;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * The starting point of the Kafka-source plugin and the Kafka consumer
 * properties and kafka multithreaded consumers are being handled here.
 */

@SuppressWarnings("deprecation")
@DataPrepperPlugin(name = "kafka", pluginType = Source.class, pluginConfigurationType = KafkaSourceConfig.class)
public class KafkaSource implements Source<Record<Event>> {
    private static final Logger LOG = LoggerFactory.getLogger(KafkaSource.class);
    private final KafkaSourceConfig sourceConfig;
    private AtomicBoolean shutdownInProgress;
    private ExecutorService executorService;
    private final PluginMetrics pluginMetrics;
    private String pipelineName;
    private static final String SCHEMA_TYPE = "schemaType";
    private final AcknowledgementSetManager acknowledgementSetManager;
    private final List<ExecutorService> allTopicExecutorServices;
    private final List<KafkaSourceCustomConsumer> allTopicConsumers;
    private final KafkaSourceCustomConsumerFactory kafkaSourceCustomConsumerFactory;

    @DataPrepperPluginConstructor
    public KafkaSource(final KafkaSourceConfig sourceConfig,
                       final PluginMetrics pluginMetrics,
                       final AcknowledgementSetManager acknowledgementSetManager,
                       final PipelineDescription pipelineDescription) {
        this.sourceConfig = sourceConfig;
        this.pluginMetrics = pluginMetrics;
        this.acknowledgementSetManager = acknowledgementSetManager;
        this.pipelineName = pipelineDescription.getPipelineName();
        this.shutdownInProgress = new AtomicBoolean(false);
        this.allTopicExecutorServices = new ArrayList<>();
        this.allTopicConsumers = new ArrayList<>();
        this.kafkaSourceCustomConsumerFactory = new KafkaSourceCustomConsumerFactory();
    }

    @Override
    public void start(Buffer<Record<Event>> buffer) {
        sourceConfig.getTopics().forEach(topic -> {
            int numWorkers = topic.getWorkers();
            executorService = Executors.newFixedThreadPool(numWorkers);
            allTopicExecutorServices.add(executorService);

            final List<KafkaSourceCustomConsumer> consumers = kafkaSourceCustomConsumerFactory.createConsumersForTopic(sourceConfig, topic, buffer,
                    pluginMetrics, acknowledgementSetManager, shutdownInProgress);
            allTopicConsumers.addAll(consumers);
            consumers.forEach(executorService::submit);

            LOG.info("Started Kafka source for topic " + topic.getName());
        });
    }

    @Override
    public void stop() {
        shutdownInProgress.set(true);
        final long shutdownWaitTime = calculateLongestThreadWaitingTime();

        LOG.info("Shutting down {} Executor services", allTopicExecutorServices.size());
        allTopicExecutorServices.forEach(executor -> stopExecutor(executor, shutdownWaitTime));

        LOG.info("Closing {} consumers", allTopicConsumers.size());
        allTopicConsumers.forEach(consumer -> consumer.closeConsumer());

        LOG.info("Kafka source shutdown successfully...");
    }

    public void stopExecutor(final ExecutorService executorService, final long shutdownWaitTime) {
        executorService.shutdown();
        try {
            if (!executorService.awaitTermination(shutdownWaitTime, TimeUnit.SECONDS)) {
                LOG.info("Consumer threads are waiting for shutting down...");
                executorService.shutdownNow();
            }
        } catch (InterruptedException e) {
            if (e.getCause() instanceof InterruptedException) {
                LOG.error("Interrupted during consumer shutdown, exiting uncleanly...", e);
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }
        }
    }

    private long calculateLongestThreadWaitingTime() {
        List<TopicConfig> topicsList = sourceConfig.getTopics();
        return topicsList.stream().
                map(
                        topics -> topics.getThreadWaitingTime().toSeconds()
                ).
                max(Comparator.comparingLong(time -> time)).
                orElse(1L);
    }

    private static boolean validateURL(String url) {
        try {
            URI uri = new URI(url);
            if (uri.getScheme() == null || uri.getHost() == null) {
                return false;
            }
            return true;
        } catch (URISyntaxException ex) {
            LOG.error("Invalid Schema Registry URI: ", ex);
            return false;
        }
    }



    private static String getSchemaType(final String registryUrl, final String topicName, final int schemaVersion) {
        StringBuilder response = new StringBuilder();
        String schemaType = MessageFormat.PLAINTEXT.toString();
        try {
            String urlPath = registryUrl + "subjects/" + topicName + "-value/versions/" + schemaVersion;
            URL url = new URL(urlPath);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            int responseCode = connection.getResponseCode();
            if (responseCode == HttpURLConnection.HTTP_OK) {
                BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()));
                String inputLine;
                while ((inputLine = reader.readLine()) != null) {
                    response.append(inputLine);
                }
                reader.close();
                ObjectMapper mapper = new ObjectMapper();
                Object json = mapper.readValue(response.toString(), Object.class);
                String indented = mapper.writerWithDefaultPrettyPrinter().writeValueAsString(json);
                JsonNode rootNode = mapper.readTree(indented);
                // If the entry exists but schema type doesn't exist then
                // the schemaType defaults to AVRO
                if (rootNode.has(SCHEMA_TYPE)) {
                    JsonNode node = rootNode.findValue(SCHEMA_TYPE);
                    schemaType = node.textValue();
                } else {
                    schemaType = MessageFormat.AVRO.toString();
                }
            } else {
                InputStream errorStream = connection.getErrorStream();
                String errorMessage = readErrorMessage(errorStream);
                // Plaintext is not a valid schematype in schema registry
                // So, if it doesn't exist in schema regitry, default
                // the schemaType to PLAINTEXT
                LOG.error("GET request failed while fetching the schema registry. Defaulting to schema type PLAINTEXT");
                return MessageFormat.PLAINTEXT.toString();
            }
        } catch (IOException e) {
            LOG.error("An error while fetching the schema registry details : ", e);
            throw new RuntimeException();
        }
        return schemaType;
    }

    private static String readErrorMessage(InputStream errorStream) throws IOException {
        if (errorStream == null) {
            return null;
        }
        BufferedReader reader = new BufferedReader(new InputStreamReader(errorStream));
        StringBuilder errorMessage = new StringBuilder();
        String line;
        while ((line = reader.readLine()) != null) {
            errorMessage.append(line);
        }
        reader.close();
        errorStream.close();
        return errorMessage.toString();
    }



    private void isTopicExists(String topicName, String bootStrapServer, Properties properties) {
        List<String> bootStrapServers = new ArrayList<>();
        String servers[];
        if (bootStrapServer.contains(",")) {
            servers = bootStrapServer.split(",");
            bootStrapServers.addAll(Arrays.asList(servers));
        } else {
            bootStrapServers.add(bootStrapServer);
        }
        properties.put("connections.max.idle.ms", 5000);
        properties.put("request.timeout.ms", 10000);
        try (AdminClient client = KafkaAdminClient.create(properties)) {
            boolean topicExists = client.listTopics().names().get().stream().anyMatch(name -> name.equalsIgnoreCase(topicName));
        } catch (InterruptedException | ExecutionException e) {
            if (e.getCause() instanceof UnknownTopicOrPartitionException) {
                LOG.error("Topic does not exist: " + topicName);
            }
            throw new RuntimeException("Exception while checking the topics availability...");
        }
    }

    private boolean isKafkaClusterExists(String bootStrapServers) {
        Socket socket = null;
        String[] serverDetails = new String[0];
        String[] servers = new String[0];
        int counter = 0;
        try {
            if (bootStrapServers.contains(",")) {
                servers = bootStrapServers.split(",");
            } else {
                servers = new String[]{bootStrapServers};
            }
            if (CollectionUtils.isNotEmpty(Arrays.asList(servers))) {
                for (String bootstrapServer : servers) {
                    if (bootstrapServer.contains(":")) {
                        serverDetails = bootstrapServer.split(":");
                        if (StringUtils.isNotEmpty(serverDetails[0])) {
                            InetAddress inetAddress = InetAddress.getByName(serverDetails[0]);
                            socket = new Socket(inetAddress, Integer.parseInt(serverDetails[1]));
                        }
                    }
                }
            }
        } catch (IOException e) {
            counter++;
            LOG.error("Kafka broker : {} is not available...", getMaskedBootStrapDetails(serverDetails[0]));
        } finally {
            if (socket != null) {
                try {
                    socket.close();
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
        if (counter == servers.length) {
            return true;
        }
        return false;
    }

    private String getMaskedBootStrapDetails(String serverIP) {
        if (serverIP == null || serverIP.length() <= 4) {
            return serverIP;
        }
        int maskedLength = serverIP.length() - 4;
        StringBuilder maskedString = new StringBuilder(maskedLength);
        for (int i = 0; i < maskedLength; i++) {
            maskedString.append('*');
        }
        return maskedString.append(serverIP.substring(maskedLength)).toString();
    }
}