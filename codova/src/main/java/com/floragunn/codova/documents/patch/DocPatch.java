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

package com.floragunn.codova.documents.patch;

import com.floragunn.codova.documents.DocNode;
import com.floragunn.codova.documents.DocType;
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.UnparsedDoc;
import com.floragunn.codova.validation.ConfigValidationException;
import com.floragunn.codova.validation.errors.ValidationError;

public interface DocPatch extends Document {
    DocNode apply(DocNode targetDocument);
    String getMediaType();

    static DocPatch parse(String contentType, String content) throws ConfigValidationException {
        if (contentType.equalsIgnoreCase(MergePatch.MEDIA_TYPE)) {
            return new MergePatch(DocNode.parse(DocType.JSON).from(content));
        } else {
            throw new ConfigValidationException(new ValidationError(null, "Unsupported patch type: " + contentType));
        }
    }

    static DocPatch parse(UnparsedDoc<?> unparsedDoc) throws ConfigValidationException {
        if (unparsedDoc.getMediaType().equalsIgnoreCase(MergePatch.MEDIA_TYPE)) {
            return new MergePatch(DocNode.parse(unparsedDoc));
        } else {
            throw new ConfigValidationException(new ValidationError(null, "Unsupported patch type: " + unparsedDoc.getMediaType()));
        }
    }

    static DocPatch parseTyped(DocNode docNode) throws ConfigValidationException {
        String contentType = docNode.getAsString("type");

        if (contentType.equalsIgnoreCase(MergePatch.MEDIA_TYPE)) {
            return new MergePatch(docNode.getAsNode("content"));
        } else {
            throw new ConfigValidationException(new ValidationError(null, "Unsupported patch type: " + contentType));
        }
    }
}
