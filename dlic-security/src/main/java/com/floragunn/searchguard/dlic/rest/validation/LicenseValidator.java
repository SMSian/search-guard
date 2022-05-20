/*
 * Copyright 2016-2017 by floragunn GmbH - All rights reserved
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
package com.floragunn.searchguard.dlic.rest.validation;

import org.opensearch.common.bytes.BytesReference;
import org.opensearch.common.settings.Settings;
import org.opensearch.rest.RestRequest;

public class LicenseValidator extends AbstractConfigurationValidator {

	public LicenseValidator(final RestRequest request, BytesReference ref, final Settings esSettings, Object... param) {
		super(request, ref, esSettings, param);
		this.payloadMandatory = true;
		allowedKeys.put("sg_license", DataType.STRING);
	}

}

