/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.integration;

import io.strimzi.kafka.metrics.prometheus.ClientMetricsReporter;
import io.strimzi.kafka.metrics.prometheus.MetricsUtils;
import io.strimzi.test.container.StrimziKafkaCluster;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.testcontainers.containers.GenericContainer;
import org.testcontainers.containers.Network;
import org.testcontainers.containers.wait.strategy.Wait;
import org.testcontainers.images.builder.Transferable;
import org.testcontainers.utility.MountableFile;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

/**
 * Pushes the metrics of a producer to an OpenTelemetry Collector and reads them back from the collector's
 * Prometheus exporter, to check that the names survive the conversion to OTLP and back.
 */
public class TestOtlpMetricsIT {

    private static final String COLLECTOR_IMAGE = "otel/opentelemetry-collector:0.162.0";
    private static final String COLLECTOR_HOST = "otel-collector";
    private static final int PROMETHEUS_EXPORTER_PORT = 8889;
    private static final String COLLECTOR_CONFIG = String.join("\n",
            "receivers:",
            "  otlp:",
            "    protocols:",
            "      grpc:",
            "        endpoint: 0.0.0.0:4317",
            "      http:",
            "        endpoint: 0.0.0.0:4318",
            "exporters:",
            "  prometheus:",
            "    endpoint: 0.0.0.0:" + PROMETHEUS_EXPORTER_PORT,
            "    resource_to_telemetry_conversion:",
            "      enabled: true",
            "service:",
            "  pipelines:",
            "    metrics:",
            "      receivers: [otlp]",
            "      exporters: [prometheus]",
            "");

    private StrimziKafkaCluster cluster;
    private GenericContainer<?> collector;
    private Map<String, String> env;

    @BeforeEach
    public void setUp() {
        cluster = new StrimziKafkaCluster.StrimziKafkaClusterBuilder()
                .withNumberOfBrokers(1)
                .withSharedNetwork()
                .build();
        cluster.start();

        collector = new GenericContainer<>(COLLECTOR_IMAGE)
                .withNetwork(Network.SHARED)
                .withNetworkAliases(COLLECTOR_HOST)
                .withExposedPorts(PROMETHEUS_EXPORTER_PORT)
                .withCopyToContainer(Transferable.of(COLLECTOR_CONFIG), "/etc/otelcol/config.yaml")
                .withCommand("--config=/etc/otelcol/config.yaml")
                .waitingFor(Wait.forListeningPort());
        collector.start();

        env = new HashMap<>();
        env.put("CLIENT_TYPE", "KafkaProducer");
        env.put("BOOTSTRAP_SERVERS", cluster.getNetworkBootstrapServers());
        env.put("TOPIC", "my-topic");
        env.put("CLASSPATH", MetricsUtils.MOUNT_PATH + "*");
        env.put("MESSAGE_COUNT", "1000");
        env.put("DELAY_MS", "100");
    }

    @AfterEach
    public void tearDown() {
        if (collector != null) {
            collector.stop();
        }
        cluster.stop();
    }

    @Test
    public void testProducerMetricsPushedOverGrpc() {
        testProducerMetricsPushedOverOtlp("grpc", 4317);
    }

    @Test
    public void testProducerMetricsPushedOverHttp() {
        testProducerMetricsPushedOverOtlp("http/protobuf", 4318);
    }

    private void testProducerMetricsPushedOverOtlp(String protocol, int port) {
        env.put("ADDITIONAL_CONFIG", String.join("\n",
                "metric.reporters=" + ClientMetricsReporter.class.getName(),
                "prometheus.metrics.reporter.listener.enable=false",
                "prometheus.metrics.reporter.otlp.enable=true",
                "prometheus.metrics.reporter.otlp.endpoint=http://" + COLLECTOR_HOST + ":" + port,
                "prometheus.metrics.reporter.otlp.protocol=" + protocol,
                "prometheus.metrics.reporter.otlp.interval.seconds=1",
                "prometheus.metrics.reporter.otlp.resource.attributes=k8s.pod.name=my-producer,service.name=my-service"));

        try (GenericContainer<?> producer = new GenericContainer<>(MetricsUtils.CLIENTS_IMAGE)
                .withNetwork(Network.SHARED)
                .withCopyFileToContainer(MountableFile.forHostPath(MetricsUtils.REPORTER_JARS), MetricsUtils.MOUNT_PATH)
                .withEnv(env)) {
            producer.start();

            List<String> patterns = List.of(
                    "kafka_producer_kafka_metrics_.*",
                    "kafka_producer_producer_metrics_.*",
                    "kafka_producer_producer_topic_metrics_.*",
                    // Gauge with a unit: the exporter moves the unit to the metadata and the collector adds it back
                    "jvm_memory_used_bytes\\{.*",
                    // Counter: the exporter drops the _total suffix and the collector adds it back
                    "jvm_threads_started_total\\{.*",
                    // Resource attributes are converted to labels by the collector
                    ".*k8s_pod_name=\"my-producer\".*",
                    ".*job=\"my-service\".*");
            MetricsUtils.verify(collector, patterns, PROMETHEUS_EXPORTER_PORT, metrics -> assertFalse(metrics.isEmpty()));

            // The suffix is not added twice
            MetricsUtils.verify(collector, List.of("jvm_threads_started_total_total.*"), PROMETHEUS_EXPORTER_PORT,
                    metrics -> assertTrue(metrics.isEmpty()));
        }
    }
}
