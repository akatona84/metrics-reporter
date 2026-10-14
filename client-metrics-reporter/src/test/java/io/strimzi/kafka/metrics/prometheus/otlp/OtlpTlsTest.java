/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.otlp;

import org.apache.kafka.common.config.ConfigException;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

public class OtlpTlsTest {

    private static final String PREFIX = "prometheus.metrics.reporter.";

    public static String resource(String name) throws IOException {
        try (var stream = OtlpTlsTest.class.getResourceAsStream("/otlp-tls/" + name)) {
            return new String(stream.readAllBytes(), StandardCharsets.UTF_8);
        }
    }

    @Test
    public void testNone() {
        assertSame(OtlpTls.NONE, OtlpTls.fromConfig(PREFIX, Map.of()));
        assertTrue(OtlpTls.NONE.isEmpty());
    }

    @Test
    public void testInline() throws IOException {
        Map<String, String> config = new HashMap<>();
        config.put(OtlpTls.TRUSTSTORE_CERTIFICATES, resource("ca.crt"));
        config.put(OtlpTls.KEYSTORE_CERTIFICATE_CHAIN, resource("client.crt"));
        config.put(OtlpTls.KEYSTORE_KEY, resource("client.key"));

        OtlpTls tls = OtlpTls.fromConfig(PREFIX, config);
        assertFalse(tls.isEmpty());
        assertEquals(resource("ca.crt"), tls.trustedCertificates());
        assertEquals(resource("client.crt"), tls.certificateChain());
        assertEquals(resource("client.key"), tls.privateKey());
        assertEquals(tls, OtlpTls.fromConfig(PREFIX, config));
    }

    @Test
    public void testLocations(@TempDir Path dir) throws IOException {
        Path truststore = Files.writeString(dir.resolve("ca.crt"), resource("ca.crt"));
        // Like the Kafka PEM keystore, the file contains both the private key and the certificate chain
        Path keystore = Files.writeString(dir.resolve("keystore.pem"), resource("client.key") + resource("client.crt"));

        OtlpTls tls = OtlpTls.fromConfig(PREFIX, Map.of(
                OtlpTls.TRUSTSTORE_LOCATION, truststore.toString(),
                OtlpTls.KEYSTORE_LOCATION, keystore.toString()));

        assertEquals(resource("ca.crt").trim(), tls.trustedCertificates().trim());
        assertEquals(resource("client.crt").trim(), tls.certificateChain().trim());
        assertEquals(resource("client.key").trim(), tls.privateKey().trim());
    }

    @Test
    public void testTrustedCertificatesOnly() throws IOException {
        OtlpTls tls = OtlpTls.fromConfig(PREFIX, Map.of(OtlpTls.TRUSTSTORE_CERTIFICATES, resource("ca.crt")));

        assertFalse(tls.isEmpty());
        assertNull(tls.certificateChain());
        assertNull(tls.privateKey());
    }

    @Test
    public void testValidation(@TempDir Path dir) throws IOException {
        String ca = resource("ca.crt");
        String chain = resource("client.crt");
        String key = resource("client.key");

        // Location and inline values cannot be set together
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(
                OtlpTls.TRUSTSTORE_LOCATION, "/tmp/ca.crt",
                OtlpTls.TRUSTSTORE_CERTIFICATES, ca)));
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(
                OtlpTls.KEYSTORE_LOCATION, "/tmp/keystore.pem",
                OtlpTls.KEYSTORE_KEY, key)));

        // The certificate chain and the key must be set together
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(OtlpTls.KEYSTORE_CERTIFICATE_CHAIN, chain)));
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(OtlpTls.KEYSTORE_KEY, key)));

        // The values must be PEM
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(OtlpTls.TRUSTSTORE_CERTIFICATES, "not a certificate")));
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(
                OtlpTls.KEYSTORE_CERTIFICATE_CHAIN, chain,
                OtlpTls.KEYSTORE_KEY, chain)));

        // The keystore file must contain both the key and the certificate chain
        Path keyOnly = Files.writeString(dir.resolve("key.pem"), key);
        assertThrows(ConfigException.class, () -> OtlpTls.fromConfig(PREFIX, Map.of(OtlpTls.KEYSTORE_LOCATION, keyOnly.toString())));

        ConfigException e = assertThrows(ConfigException.class,
                () -> OtlpTls.fromConfig(PREFIX, Map.of(OtlpTls.TRUSTSTORE_LOCATION, dir.resolve("missing.crt").toString())));
        assertTrue(e.getMessage().contains(PREFIX + OtlpTls.TRUSTSTORE_LOCATION));
    }

    @Test
    public void testToStringDoesNotContainTheKey() throws IOException {
        OtlpTls tls = OtlpTls.fromConfig(PREFIX, Map.of(
                OtlpTls.KEYSTORE_CERTIFICATE_CHAIN, resource("client.crt"),
                OtlpTls.KEYSTORE_KEY, resource("client.key")));

        assertFalse(tls.toString().contains("PRIVATE KEY"));
        assertFalse(new OtlpSettings(null, null, null, Map.of(), tls).toString().contains("PRIVATE KEY"));
    }

    @Test
    public void testPem() throws IOException {
        String keystore = resource("client.key") + "\nsome text\n" + resource("client.crt") + resource("ca.crt");

        assertEquals(2, OtlpTls.pem(keystore, "CERTIFICATE").split("-----BEGIN CERTIFICATE-----").length - 1);
        assertEquals(1, OtlpTls.pem(keystore, "PRIVATE KEY").split("-----BEGIN PRIVATE KEY-----").length - 1);
        assertEquals("", OtlpTls.pem("some text", "CERTIFICATE"));
    }
}
