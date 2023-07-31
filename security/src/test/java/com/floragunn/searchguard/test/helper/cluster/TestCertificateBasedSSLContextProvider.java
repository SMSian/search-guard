/*
 * Copyright 2021 floragunn GmbH
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package com.floragunn.searchguard.test.helper.cluster;

import javax.net.ssl.SSLContext;

import com.floragunn.codova.config.net.TLSConfig;
import com.floragunn.searchguard.test.helper.certificate.TestCertificate;

public class TestCertificateBasedSSLContextProvider implements SSLContextProvider {

    private final TestCertificate caCertificate;
    private final TestCertificate certificate;

    public TestCertificateBasedSSLContextProvider(TestCertificate caCertificate, TestCertificate certificate) {
        this.caCertificate = caCertificate;
        this.certificate = certificate;
    }

    @Override
    public SSLContext getSslContext(boolean clientAuthentication) {
        try {
            TLSConfig.Builder tlsConfigBuilder = new TLSConfig.Builder().trust(caCertificate.getCertificateFile());

            if (clientAuthentication) {
                tlsConfigBuilder = tlsConfigBuilder.clientCert(certificate.getCertificateFile(), certificate.getPrivateKeyFile(),
                        certificate.getPrivateKeyPassword());
            }

            return tlsConfigBuilder.build().getUnrestrictedSslContext();
        } catch (Exception e) {
            throw new RuntimeException("Error when building SSLContext for tests", e);
        }
    }
}
