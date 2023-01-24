/*
  * Copyright 2016-2018 by floragunn GmbH - All rights reserved
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed here is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *
 * This software is free of charge for non-commercial and academic use.
 * For commercial use in a production environment you have to obtain a license
 * from https://floragunn.com
 *
 */
package com.floragunn.searchguard.enterprise.auth.oidc;

import com.floragunn.searchguard.authc.AuthenticatorUnavailableException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKey;
import org.apache.cxf.rs.security.jose.jwk.JsonWebKeys;
import org.junit.Assert;
import org.junit.Test;

public class SelfRefreshingKeySetTest {

    @Test
    public void basicTest() throws AuthenticatorUnavailableException, BadCredentialsException {
        SelfRefreshingKeySet selfRefreshingKeySet = new SelfRefreshingKeySet(new MockKeySetProvider());

        JsonWebKey key1 = selfRefreshingKeySet.getKey("kid/a");
        Assert.assertEquals(TestJwk.OCT_1_K, key1.getProperty("k"));
        Assert.assertEquals(1, selfRefreshingKeySet.getRefreshCount());

        JsonWebKey key2 = selfRefreshingKeySet.getKey("kid/b");
        Assert.assertEquals(TestJwk.OCT_2_K, key2.getProperty("k"));
        Assert.assertEquals(1, selfRefreshingKeySet.getRefreshCount());

        try {
            selfRefreshingKeySet.getKey("kid/X");
            Assert.fail("Expected a BadCredentialsException");
        } catch (BadCredentialsException e) {
            Assert.assertEquals(2, selfRefreshingKeySet.getRefreshCount());
        }

    }

    @Test(timeout = 10000)
    public void twoThreadedTest() throws Exception {
        BlockingMockKeySetProvider provider = new BlockingMockKeySetProvider();

        final SelfRefreshingKeySet selfRefreshingKeySet = new SelfRefreshingKeySet(provider);

        ExecutorService executorService = Executors.newCachedThreadPool();

        Future<JsonWebKey> f1 = executorService.submit(() -> selfRefreshingKeySet.getKey("kid/a"));

        provider.waitForCalled();

        Future<JsonWebKey> f2 = executorService.submit(() -> selfRefreshingKeySet.getKey("kid/b"));

        while (selfRefreshingKeySet.getQueuedGetCount() == 0) {
            Thread.sleep(10);
        }

        provider.unblock();

        Assert.assertEquals(TestJwk.OCT_1_K, f1.get().getProperty("k"));
        Assert.assertEquals(TestJwk.OCT_2_K, f2.get().getProperty("k"));

        Assert.assertEquals(1, selfRefreshingKeySet.getRefreshCount());
        Assert.assertEquals(1, selfRefreshingKeySet.getQueuedGetCount());

    }

    static class MockKeySetProvider implements KeySetProvider {

        @Override
        public JsonWebKeys get() throws AuthenticatorUnavailableException {
            return TestJwk.OCT_1_2_3;
        }

    }

    static class BlockingMockKeySetProvider extends MockKeySetProvider {
        private boolean blocked = true;
        private boolean called = false;

        @Override
        public synchronized JsonWebKeys get() throws AuthenticatorUnavailableException {

            called = true;
            notifyAll();

            waitForUnblock();

            return super.get();
        }

        public synchronized void unblock() {
            blocked = false;
            notifyAll();
        }

        public synchronized void waitForCalled() throws InterruptedException {
            while (!called) {
                wait();
            }
        }

        private synchronized void waitForUnblock() {
            while (blocked) {
                try {
                    wait();
                } catch (InterruptedException e) {
                }

            }
        }
    }
}
