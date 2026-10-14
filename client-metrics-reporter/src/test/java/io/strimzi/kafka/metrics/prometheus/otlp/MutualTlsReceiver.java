/*
 * Copyright Strimzi authors.
 * License: Apache License 2.0 (see the file LICENSE or http://apache.org/licenses/LICENSE-2.0.html).
 */
package io.strimzi.kafka.metrics.prometheus.otlp;

import com.sun.net.httpserver.HttpsConfigurator;
import com.sun.net.httpserver.HttpsParameters;
import com.sun.net.httpserver.HttpsServer;

import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.TrustManagerFactory;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.security.KeyFactory;
import java.security.KeyStore;
import java.security.PrivateKey;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.security.spec.PKCS8EncodedKeySpec;
import java.util.Base64;
import java.util.concurrent.BlockingQueue;

/**
 * Minimal OTLP/HTTP receiver over TLS, which requires a client certificate signed by the test CA.
 */
class MutualTlsReceiver {

    private MutualTlsReceiver() { }

    static HttpsServer start(BlockingQueue<String> requests) throws Exception {
        CertificateFactory certificateFactory = CertificateFactory.getInstance("X.509");
        Certificate ca = certificateFactory.generateCertificate(
                new ByteArrayInputStream(OtlpTlsTest.resource("ca.crt").getBytes(StandardCharsets.UTF_8)));
        Certificate serverCertificate = certificateFactory.generateCertificate(
                new ByteArrayInputStream(OtlpTlsTest.resource("server.crt").getBytes(StandardCharsets.UTF_8)));
        String serverKeyPem = OtlpTlsTest.resource("server.key")
                .replaceAll("-----(BEGIN|END) PRIVATE KEY-----", "")
                .replaceAll("\\s", "");
        PrivateKey serverKey = KeyFactory.getInstance("RSA")
                .generatePrivate(new PKCS8EncodedKeySpec(Base64.getDecoder().decode(serverKeyPem)));

        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        keyStore.load(null, null);
        keyStore.setKeyEntry("server", serverKey, new char[0], new Certificate[] {serverCertificate, ca});
        KeyManagerFactory keyManagers = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
        keyManagers.init(keyStore, new char[0]);

        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        trustStore.load(null, null);
        trustStore.setCertificateEntry("ca", ca);
        TrustManagerFactory trustManagers = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagers.init(trustStore);

        SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers.getKeyManagers(), trustManagers.getTrustManagers(), null);

        HttpsServer server = HttpsServer.create(new InetSocketAddress("localhost", 0), 0);
        server.setHttpsConfigurator(new HttpsConfigurator(sslContext) {
            @Override
            public void configure(HttpsParameters params) {
                SSLParameters parameters = sslContext.getDefaultSSLParameters();
                parameters.setNeedClientAuth(true);
                params.setSSLParameters(parameters);
            }
        });
        server.createContext("/v1/metrics", exchange -> {
            try (InputStream body = exchange.getRequestBody()) {
                requests.add(new String(body.readAllBytes(), StandardCharsets.ISO_8859_1));
            }
            exchange.sendResponseHeaders(200, -1);
            exchange.close();
        });
        server.start();
        return server;
    }
}
