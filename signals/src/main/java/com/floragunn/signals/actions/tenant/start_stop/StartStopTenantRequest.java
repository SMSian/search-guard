/*
 * Copyright 2023 floragunn GmbH
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
package com.floragunn.signals.actions.tenant.start_stop;

import java.io.IOException;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class StartStopTenantRequest extends ActionRequest {

    private boolean activate;

    public StartStopTenantRequest() {
        super();
    }

    public StartStopTenantRequest(boolean activate) {
        super();
        this.activate = activate;
    }

    public StartStopTenantRequest(StreamInput in) throws IOException {
        super(in);
        this.activate = in.readBoolean();
    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeBoolean(activate);
    }

    @Override
    public ActionRequestValidationException validate() {

        return null;
    }

    public boolean isActivate() {
        return activate;
    }

    public void setActivate(boolean activate) {
        this.activate = activate;
    }

}
