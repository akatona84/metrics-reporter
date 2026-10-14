/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.otlp;

import com.sun.net.httpserver.HttpServer;
import com.sun.net.httpserver.HttpsServer;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import io.prometheus.metrics.model.snapshots.GaugeSnapshot;
import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OtlpExportersTest {

    private static final List<String> OTEL_PROPERTIES = List.of(
            "otel.exporter.otlp.endpoint",
            "otel.exporter.otlp.metrics.endpoint",
            "otel.exporter.otlp.protocol",
            "otel.metric.export.interval",
            "otel.resource.attributes");

    private final PrometheusRegistry registry = new PrometheusRegistry();
    private final BlockingQueue<String> requests = new LinkedBlockingQueue<>();
    private HttpServer receiver;
    private String endpoint;

    @BeforeEach
    public void setUp() throws IOException {
        // Minimal OTLP/HTTP receiver: protobuf encodes strings as raw UTF-8, so metric names and resource attributes
        // can be asserted on without decoding the payload.
        receiver = HttpServer.create(new InetSocketAddress("localhost", 0), 0);
        receiver.createContext("/v1/metrics", exchange -> {
            try (InputStream body = exchange.getRequestBody()) {
                requests.add(new String(body.readAllBytes(), StandardCharsets.ISO_8859_1));
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        receiver.start();
        endpoint = "http://localhost:" + receiver.getAddress().getPort();

        registry.register(() -> GaugeSnapshot.builder()
                .name("kafka_server_replicamanager_underreplicatedpartitions")
                .dataPoint(GaugeSnapshot.GaugeDataPointSnapshot.builder().value(3).build())
                .build());
    }

    @AfterEach
    public void tearDown() {
        receiver.stop(0);
        OTEL_PROPERTIES.forEach(System::clearProperty);
    }

    @Test
    public void testLifecycle() {
        OtlpSettings settings1 = new OtlpSettings(endpoint, "http/protobuf", 60, Map.of("a", "1"));
        OtlpExporters.ExporterCounter exporter1 = OtlpExporters.getOrCreate(settings1, registry);

        OtlpSettings settings2 = new OtlpSettings(endpoint, "http/protobuf", 60, Map.of("a", "1"));
        OtlpExporters.ExporterCounter exporter2 = OtlpExporters.getOrCreate(settings2, registry);
        assertSame(exporter1, exporter2);

        OtlpSettings settings3 = new OtlpSettings(endpoint, "http/protobuf", 60, Map.of("a", "2"));
        OtlpExporters.ExporterCounter exporter3 = OtlpExporters.getOrCreate(settings3, registry);
        assertNotSame(exporter1, exporter3);

        OtlpExporters.release(exporter1);
        assertSame(exporter2, OtlpExporters.getOrCreate(settings2, registry));
        OtlpExporters.release(exporter2);
        OtlpExporters.release(exporter2);

        OtlpExporters.ExporterCounter exporter4 = OtlpExporters.getOrCreate(settings1, registry);
        assertNotSame(exporter1, exporter4);

        OtlpExporters.release(exporter3);
        OtlpExporters.release(exporter4);
    }

    @Test
    public void testPushesMetricsWithResourceAttributes() throws InterruptedException {
        OtlpSettings settings = new OtlpSettings(endpoint, "http/protobuf", 1,
                Map.of("k8s.pod.name", "my-cluster-kafka-0", "k8s.namespace.name", "my-namespace"));
        OtlpExporters.ExporterCounter exporter = OtlpExporters.getOrCreate(settings, registry);
        try {
            String request = requests.poll(30, TimeUnit.SECONDS);
            assertNotNull(request, "No OTLP request received");
            assertTrue(request.contains("kafka_server_replicamanager_underreplicatedpartitions"));
            assertTrue(request.contains("k8s.pod.name"));
            assertTrue(request.contains("my-cluster-kafka-0"));
            assertTrue(request.contains("k8s.namespace.name"));
            assertTrue(request.contains("my-namespace"));
        } finally {
            OtlpExporters.release(exporter);
        }
    }

    @Test
    public void testSettingsTakePrecedenceOverOpenTelemetryConfiguration() throws InterruptedException {
        // Typically set for tracing, they must not redirect the metrics away from the configured endpoint
        System.setProperty("otel.exporter.otlp.endpoint", "http://localhost:1");
        System.setProperty("otel.exporter.otlp.metrics.endpoint", "http://localhost:1");
        System.setProperty("otel.exporter.otlp.protocol", "grpc");
        System.setProperty("otel.resource.attributes", "k8s.pod.name=from-otel,deployment.environment.name=from-env");

        OtlpSettings settings = new OtlpSettings(endpoint, "http/protobuf", 1, Map.of("k8s.pod.name", "from-reporter"));
        OtlpExporters.ExporterCounter exporter = OtlpExporters.getOrCreate(settings, registry);
        try {
            String request = requests.poll(30, TimeUnit.SECONDS);
            assertNotNull(request, "No OTLP request received");
            assertTrue(request.contains("from-reporter"));
            assertFalse(request.contains("from-otel"));
            // Other resource attributes from the OpenTelemetry configuration are merged
            assertTrue(request.contains("deployment.environment.name"));
            assertTrue(request.contains("from-env"));
        } finally {
            OtlpExporters.release(exporter);
        }
    }

    @Test
    public void testUnsetSettingsUseOpenTelemetryConfiguration() throws InterruptedException {
        System.setProperty("otel.exporter.otlp.endpoint", endpoint);
        System.setProperty("otel.exporter.otlp.protocol", "http/protobuf");
        System.setProperty("otel.metric.export.interval", "1000");

        OtlpSettings settings = new OtlpSettings(null, null, null, Map.of());
        OtlpExporters.ExporterCounter exporter = OtlpExporters.getOrCreate(settings, registry);
        try {
            String request = requests.poll(30, TimeUnit.SECONDS);
            assertNotNull(request, "No OTLP request received");
            assertTrue(request.contains("kafka_server_replicamanager_underreplicatedpartitions"));
        } finally {
            OtlpExporters.release(exporter);
        }
    }

    @Test
    public void testPushesMetricsWithMutualTls() throws Exception {
        HttpsServer httpsReceiver = MutualTlsReceiver.start(requests);
        try {
            OtlpTls tls = OtlpTls.fromConfig("prometheus.metrics.reporter.", Map.of(
                    OtlpTls.TRUSTSTORE_CERTIFICATES, OtlpTlsTest.resource("ca.crt"),
                    OtlpTls.KEYSTORE_CERTIFICATE_CHAIN, OtlpTlsTest.resource("client.crt"),
                    OtlpTls.KEYSTORE_KEY, OtlpTlsTest.resource("client.key")));
            OtlpSettings settings = new OtlpSettings("https://localhost:" + httpsReceiver.getAddress().getPort(),
                    "http/protobuf", 1, Map.of(), tls);
            OtlpExporters.ExporterCounter exporter = OtlpExporters.getOrCreate(settings, registry);
            try {
                String request = requests.poll(30, TimeUnit.SECONDS);
                assertNotNull(request, "No OTLP request received");
                assertTrue(request.contains("kafka_server_replicamanager_underreplicatedpartitions"));
            } finally {
                OtlpExporters.release(exporter);
            }
        } finally {
            httpsReceiver.stop(0);
        }
    }

    @Test
    public void testTlsSystemPropertiesAreRestored() throws IOException {
        System.setProperty(OtlpExporters.CERTIFICATE_PROPERTY, "previous");
        try {
            OtlpTls tls = OtlpTls.fromConfig("prometheus.metrics.reporter.", Map.of(
                    OtlpTls.TRUSTSTORE_CERTIFICATES, OtlpTlsTest.resource("ca.crt"),
                    OtlpTls.KEYSTORE_CERTIFICATE_CHAIN, OtlpTlsTest.resource("client.crt"),
                    OtlpTls.KEYSTORE_KEY, OtlpTlsTest.resource("client.key")));

            List<Path> files = OtlpExporters.withTlsSystemProperties(tls, () -> List.of(
                    Path.of(System.getProperty(OtlpExporters.CERTIFICATE_PROPERTY)),
                    Path.of(System.getProperty(OtlpExporters.CLIENT_CERTIFICATE_PROPERTY)),
                    Path.of(System.getProperty(OtlpExporters.CLIENT_KEY_PROPERTY))));

            assertEquals("previous", System.getProperty(OtlpExporters.CERTIFICATE_PROPERTY));
            assertNull(System.getProperty(OtlpExporters.CLIENT_CERTIFICATE_PROPERTY));
            assertNull(System.getProperty(OtlpExporters.CLIENT_KEY_PROPERTY));
            for (Path file : files) {
                assertFalse(Files.exists(file), "Temporary file " + file + " was not deleted");
            }
        } finally {
            System.clearProperty(OtlpExporters.CERTIFICATE_PROPERTY);
        }
    }

    @Test
    public void testMetricsEndpoint() {
        assertEquals("http://collector:4318/v1/metrics",
                OtlpExporters.metricsEndpoint(new OtlpSettings("http://collector:4318", "http/protobuf", null, Map.of())));
        assertEquals("http://collector:4318/v1/metrics",
                OtlpExporters.metricsEndpoint(new OtlpSettings("http://collector:4318/", "http/protobuf", null, Map.of())));
        assertEquals("http://collector:4318/v1/metrics",
                OtlpExporters.metricsEndpoint(new OtlpSettings("http://collector:4318/v1/metrics", "http/protobuf", null, Map.of())));
        assertEquals("http://collector:4317",
                OtlpExporters.metricsEndpoint(new OtlpSettings("http://collector:4317", "grpc", null, Map.of())));

        System.setProperty("otel.exporter.otlp.protocol", "http/protobuf");
        assertEquals("http://collector:4318/v1/metrics",
                OtlpExporters.metricsEndpoint(new OtlpSettings("http://collector:4318", null, null, Map.of())));
    }

    @Test
    public void testCheckMetricsExporter() {
        assertDoesNotThrow(() -> OtlpExporters.checkMetricsExporter(OtlpExporters.METRICS_EXPORTER_PROPERTY, null));
        assertDoesNotThrow(() -> OtlpExporters.checkMetricsExporter(OtlpExporters.METRICS_EXPORTER_PROPERTY, "otlp"));
        assertDoesNotThrow(() -> OtlpExporters.checkMetricsExporter(OtlpExporters.METRICS_EXPORTER_PROPERTY, "prometheus,otlp"));
        ConfigException e = assertThrows(ConfigException.class,
                () -> OtlpExporters.checkMetricsExporter(OtlpExporters.METRICS_EXPORTER_PROPERTY, "none"));
        assertTrue(e.getMessage().contains("otel.metrics.exporter"));
    }

    @Test
    public void testCheckCanStart() {
        assertDoesNotThrow(OtlpExporters::checkCanStart);

        System.setProperty(OtlpExporters.METRICS_EXPORTER_PROPERTY, "none");
        try {
            assertThrows(ConfigException.class, OtlpExporters::checkCanStart);
        } finally {
            System.clearProperty(OtlpExporters.METRICS_EXPORTER_PROPERTY);
        }
    }
}
