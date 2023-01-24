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

import org.apache.cxf.jaxrs.json.basic.JsonMapObject;
import org.apache.cxf.jaxrs.json.basic.JsonMapObjectReaderWriter;

class CxfTestTools {

    static String toJson(JsonMapObject jsonMapObject) {
        return new JsonMapObjectReaderWriter().toJson(jsonMapObject);
    }
}
