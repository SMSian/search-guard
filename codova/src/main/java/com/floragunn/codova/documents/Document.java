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

package com.floragunn.codova.documents;

public interface Document<T> {
    Object toBasicObject();

    default String toString(Format format) {
        return DocWriter.format(format).writeAsString(this.toBasicObject());
    }

    default byte[] toBytes(Format format) {
        return DocWriter.format(format).writeAsBytes(this.toBasicObject());        
    }

    default String toJsonString() {
        return toString(Format.JSON);
    }

    default String toYamlString() {
        return toString(Format.YAML);
    }
    
    default byte[] toSmile() {
        return toBytes(Format.SMILE);
    }

    default DocNode toDocNode() {
        Object basicObject = toBasicObject();

        if (basicObject != null) {
            return DocNode.wrap(basicObject);
        } else {
            return null;
        }
    }
    
    @SuppressWarnings("unchecked")
    static <T> Document<T> assertedType(Object object, Class<T> type) {
        return (Document<T>) DocNode.wrap(object);
    }    
}
