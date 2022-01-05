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
import com.floragunn.codova.documents.Document;
import com.floragunn.codova.documents.Parser;
import com.floragunn.codova.validation.ConfigValidationException;

public interface PatchableDocument<T> extends Document<T> {
    default T patch(DocPatch patch, Parser.Context context) throws ConfigValidationException {
        DocNode patchedDocNode = patch.apply(this.toDocNode());
        
        return parseI(patchedDocNode, context);
    }
    
    T parseI(DocNode docNode, Parser.Context context) throws ConfigValidationException;
}
