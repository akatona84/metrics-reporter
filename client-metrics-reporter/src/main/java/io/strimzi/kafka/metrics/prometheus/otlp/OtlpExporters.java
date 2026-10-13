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

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

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
                this.exporter = OpenTelemetryExporter.builder(config)
                        .registry(registry)
                        .buildAndStart();
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
