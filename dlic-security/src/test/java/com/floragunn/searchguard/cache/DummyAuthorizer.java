/*
 * Copyright 2015-2018 by floragunn GmbH - All rights reserved
 *
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

package com.floragunn.searchguard.cache;

import java.nio.file.Path;

import org.opensearch.OpenSearchSecurityException;
import org.opensearch.common.settings.Settings;

import com.floragunn.searchguard.auth.api.SyncAuthorizationBackend;
import com.floragunn.searchguard.user.AuthCredentials;
import com.floragunn.searchguard.user.User;

public class DummyAuthorizer implements SyncAuthorizationBackend {

    private static volatile long count;

    public DummyAuthorizer(final Settings settings, final Path configPath) {
    }

    @Override
    public String getType() {
        return "dummy";
    }

    @Override
    public void fillRoles(User user, AuthCredentials credentials) throws OpenSearchSecurityException {
        count++;
        user.addRole("role_" + user.getName() + "_" + System.currentTimeMillis() + "_" + count);

    }

    public static long getCount() {
        return count;
    }
    
    public static void reset() {
        count=0;
    }

}
