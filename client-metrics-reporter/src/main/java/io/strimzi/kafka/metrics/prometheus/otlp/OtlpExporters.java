/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.otlp;

import io.prometheus.metrics.config.ExporterOpenTelemetryProperties;
import io.prometheus.metrics.config.PrometheusProperties;
import io.prometheus.metrics.exporter.opentelemetry.OpenTelemetryExporter;
import io.prometheus.metrics.model.registry.PrometheusRegistry;
import org.apache.kafka.common.config.ConfigException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystems;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * Class to keep track of all the OTLP exporters started by all the Kafka components in a JVM.
 */
public class OtlpExporters {

    private final static Logger LOG = LoggerFactory.getLogger(OtlpExporters.class);
    private static final Map<OtlpSettings, ExporterCounter> EXPORTERS = new HashMap<>();

    static final String EXPORTER_CLASS = "io.prometheus.metrics.exporter.opentelemetry.OpenTelemetryExporter";
    static final String METRICS_EXPORTER_PROPERTY = "otel.metrics.exporter";
    static final String METRICS_EXPORTER_ENV_VAR = "OTEL_METRICS_EXPORTER";
    private static final String METRICS_PATH = "/v1/metrics";
    private static final String DEFAULT_PROTOCOL = "grpc";
    static final String CERTIFICATE_PROPERTY = "otel.exporter.otlp.metrics.certificate";
    static final String CLIENT_CERTIFICATE_PROPERTY = "otel.exporter.otlp.metrics.client.certificate";
    static final String CLIENT_KEY_PROPERTY = "otel.exporter.otlp.metrics.client.key";

    private OtlpExporters() {
        // utility class that shouldn't be instantiated
    }

    /**
     * Get or create a new OTLP exporter if there isn't an existing instance for the specified settings.
     * @param settings The exporter settings
     * @param registry The Prometheus registry to export
     * @return An ExporterCounter instance
     */
    public synchronized static ExporterCounter getOrCreate(OtlpSettings settings, PrometheusRegistry registry) {
        ExporterCounter exporterCounter = EXPORTERS.get(settings);
        if (exporterCounter == null) {
            exporterCounter = new ExporterCounter(settings, registry);
            EXPORTERS.put(settings, exporterCounter);
        }
        exporterCounter.count.incrementAndGet();
        return exporterCounter;
    }

    /**
     * Release an OTLP exporter instance. If no other components hold this instance, it is closed.
     * @param exporterCounter The OTLP exporter instance to release
     */
    public synchronized static void release(ExporterCounter exporterCounter) {
        if (exporterCounter.close()) {
            EXPORTERS.remove(exporterCounter.settings);
        }
    }

    /**
     * Check that the OTLP exporter can be started in this JVM.
     * The exporter dependency can be excluded by client applications, and the OpenTelemetry SDK it bundles reads the
     * JVM wide otel.metrics.exporter setting, which other OpenTelemetry users like tracing commonly set to none.
     *
     * @throws ConfigException if the OTLP exporter cannot be started
     */
    public static void checkCanStart() {
        try {
            Class.forName(EXPORTER_CLASS, false, OtlpExporters.class.getClassLoader());
        } catch (ClassNotFoundException e) {
            throw new ConfigException("Pushing metrics to an OTLP endpoint requires the " +
                    "io.prometheus:prometheus-metrics-exporter-opentelemetry dependency, which is not on the classpath");
        }
        checkMetricsExporter(METRICS_EXPORTER_PROPERTY, System.getProperty(METRICS_EXPORTER_PROPERTY));
        checkMetricsExporter(METRICS_EXPORTER_ENV_VAR, System.getenv(METRICS_EXPORTER_ENV_VAR));
    }

    /* test */ static void checkMetricsExporter(String name, String value) {
        if (value != null && !value.toLowerCase(Locale.ROOT).contains("otlp")) {
            throw new ConfigException("Pushing metrics to an OTLP endpoint is not possible because " + name +
                    " is set to " + value + ". It must be unset or include otlp.");
        }
    }

    /**
     * The configured endpoint is a base URL, like OTEL_EXPORTER_OTLP_ENDPOINT. The exporter only appends the
     * /v1/metrics path for http/protobuf to endpoints coming from the OpenTelemetry configuration, not to the ones
     * passed as Prometheus properties, so it is appended here.
     */
    /* test */ static String metricsEndpoint(OtlpSettings settings) {
        String endpoint = settings.endpoint();
        if (!"http/protobuf".equals(effectiveProtocol(settings)) || endpoint.endsWith(METRICS_PATH)) {
            return endpoint;
        }
        return endpoint.endsWith("/") ? endpoint + METRICS_PATH.substring(1) : endpoint + METRICS_PATH;
    }

    private static String effectiveProtocol(OtlpSettings settings) {
        if (settings.protocol() != null) {
            return settings.protocol();
        }
        for (String name : List.of("otel.exporter.otlp.metrics.protocol", "otel.exporter.otlp.protocol")) {
            String value = System.getProperty(name);
            if (value == null) {
                value = System.getenv(name.toUpperCase(Locale.ROOT).replace('.', '_'));
            }
            if (value != null) {
                return value;
            }
        }
        return DEFAULT_PROTOCOL;
    }

    /**
     * Run the given action with the TLS settings set as the metrics-specific OpenTelemetry system properties.
     * The Prometheus properties don't have TLS options, and the OpenTelemetry SDK only accepts the certificates and
     * the key as file paths. So the PEM values are written to temporary files only readable by the current user, and
     * the system properties are set while the exporter is created, as the SDK reads the files at that time. Both are
     * removed afterwards. This is called with the class lock held, so exporters are not created concurrently.
     */
    /* test */ static <T> T withTlsSystemProperties(OtlpTls tls, Supplier<T> action) {
        if (tls.isEmpty()) {
            return action.get();
        }

        Map<String, String> properties = new LinkedHashMap<>();
        List<Path> files = new ArrayList<>();
        try {
            if (tls.trustedCertificates() != null) {
                properties.put(CERTIFICATE_PROPERTY, writeTemporaryFile(files, tls.trustedCertificates()));
            }
            if (tls.certificateChain() != null) {
                properties.put(CLIENT_CERTIFICATE_PROPERTY, writeTemporaryFile(files, tls.certificateChain()));
                properties.put(CLIENT_KEY_PROPERTY, writeTemporaryFile(files, tls.privateKey()));
            }

            Map<String, String> previous = new HashMap<>();
            properties.forEach((name, value) -> previous.put(name, System.setProperty(name, value)));
            try {
                return action.get();
            } finally {
                previous.forEach((name, value) -> {
                    if (value == null) {
                        System.clearProperty(name);
                    } else {
                        System.setProperty(name, value);
                    }
                });
            }
        } finally {
            for (Path file : files) {
                try {
                    Files.deleteIfExists(file);
                } catch (IOException e) {
                    LOG.warn("Failed to delete the temporary file {}", file, e);
                }
            }
        }
    }

    private static String writeTemporaryFile(List<Path> files, String content) {
        try {
            Path file = FileSystems.getDefault().supportedFileAttributeViews().contains("posix")
                    ? Files.createTempFile("strimzi-otlp-", ".pem", PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-------")))
                    : Files.createTempFile("strimzi-otlp-", ".pem");
            files.add(file);
            Files.writeString(file, content, StandardCharsets.UTF_8);
            return file.toString();
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to write the OTLP TLS configuration to a temporary file", e);
        }
    }

    /**
     * Class used to keep track of the OTLP exporter started with given settings.
     */
    public static class ExporterCounter {
        private final OtlpSettings settings;
        private final AtomicInteger count;
        private final OpenTelemetryExporter exporter;

        private ExporterCounter(OtlpSettings settings, PrometheusRegistry registry) {
            // The settings are passed as Prometheus properties rather than with the builder methods, because the
            // builder values are only defaults that any OTEL_* environment variable or otel.* system property overrides.
            ExporterOpenTelemetryProperties.Builder properties = ExporterOpenTelemetryProperties.builder();
            if (settings.endpoint() != null) {
                properties.endpoint(metricsEndpoint(settings));
            }
            if (settings.protocol() != null) {
                properties.protocol(settings.protocol());
            }
            if (settings.intervalSeconds() != null) {
                properties.intervalSeconds(settings.intervalSeconds());
            }
            settings.resourceAttributes().forEach(properties::resourceAttribute);
            PrometheusProperties config = PrometheusProperties.builder()
                    .exporterOpenTelemetryProperties(properties.build())
                    .build();
            try {
                this.exporter = withTlsSystemProperties(settings.tls(), () -> OpenTelemetryExporter.builder(config)
                        .registry(registry)
                        .buildAndStart());
            } catch (RuntimeException e) {
                throw new IllegalStateException("Failed to start the OTLP exporter with " + settings, e);
            }
            this.settings = settings;
            this.count = new AtomicInteger();
            LOG.debug("Started OTLP exporter with {}", settings);
        }

        /**
         * The settings this OTLP exporter was started with.
         * @return The settings
         */
        public OtlpSettings settings() {
            return settings;
        }

        private synchronized boolean close() {
            int remaining = count.decrementAndGet();
            if (remaining == 0) {
                exporter.close();
                LOG.debug("Stopped OTLP exporter with {}", settings);
                return true;
            }
            return false;
        }
    }
}
