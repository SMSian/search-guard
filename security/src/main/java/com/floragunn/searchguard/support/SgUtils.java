/*
 * Copyright 2015-2018 floragunn GmbH
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
package com.floragunn.searchguard.support;

import java.util.Map;
import java.util.Set;

public final class SgUtils {

    public static String evalMap(final Map<String, Set<String>> map, final String index) {

        if (map == null) {
            return null;
        }

        if (map.get(index) != null) {
            return isNullSet(map.get(index)) ? null : index;
        }

        if (map.get("*") != null) {
            return isNullSet(map.get("*")) ? null : "*";
        }

        if (map.get("_all") != null) {
            return isNullSet(map.get("_all")) ? null : "_all";
        }

        //regex
        for (final String key : map.keySet()) {
            if (WildcardMatcher.containsWildcard(key) && WildcardMatcher.match(key, index)) {

                if (isNullSet(map.get(key))) {
                    return null;
                }

                return key;
            }
        }

        return null;
    }

    private static boolean isNullSet(final Set<String> set) {
        return set.size() == 1 && set.iterator().next() == null;
    }
}
