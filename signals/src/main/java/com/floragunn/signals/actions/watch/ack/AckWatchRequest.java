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
package com.floragunn.signals.actions.watch.ack;

import java.io.IOException;
import org.elasticsearch.action.ActionRequestValidationException;
import org.elasticsearch.action.support.nodes.BaseNodesRequest;
import org.elasticsearch.common.io.stream.StreamInput;
import org.elasticsearch.common.io.stream.StreamOutput;

public class AckWatchRequest extends BaseNodesRequest<AckWatchRequest> {

    private String watchId;
    private String actionId;
    private boolean ack;

    public AckWatchRequest(String watchId, String actionId, boolean ack) {
        super((String[]) null);
        this.watchId = watchId;
        this.actionId = actionId;
        this.ack = ack;
    }

    public AckWatchRequest(StreamInput in) throws IOException {
        super(in);
        this.watchId = in.readString();
        this.ack = in.readBoolean();
        this.actionId = in.readOptionalString();

    }

    @Override
    public void writeTo(final StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeString(watchId);
        out.writeBoolean(ack);
        out.writeOptionalString(actionId);

    }

    @Override
    public ActionRequestValidationException validate() {
        if (watchId == null || watchId.length() == 0) {
            return new ActionRequestValidationException();
        }
        return null;
    }

    public String getWatchId() {
        return watchId;
    }

    public void setWatchId(String watchId) {
        this.watchId = watchId;
    }

    public String getActionId() {
        return actionId;
    }

    public void setActionId(String actionId) {
        this.actionId = actionId;
    }

    public boolean isAck() {
        return ack;
    }

    public void setAck(boolean ack) {
        this.ack = ack;
    }

}
