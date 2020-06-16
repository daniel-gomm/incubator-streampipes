/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.rest.management;

import org.apache.streampipes.manager.operations.Operations;
import org.apache.streampipes.model.client.messages.Notification;
import org.apache.streampipes.model.client.messages.NotificationType;
import org.apache.streampipes.model.client.pipeline.Pipeline;
import org.apache.streampipes.model.client.pipeline.PipelineOperationStatus;
import org.apache.streampipes.rest.impl.AbstractRestInterface;

import javax.ws.rs.core.Response;

public class PipelineManagement extends AbstractRestInterface {

    public Response stopPipeline(String pipelineId) {
        try {
            Pipeline pipeline = getPipelineStorage().getPipeline(pipelineId);
            PipelineOperationStatus status = Operations.stopPipeline(pipeline);
            return ok(status);
        } catch
                (Exception e) {
            e.printStackTrace();
            return constructErrorMessage(new Notification(NotificationType.UNKNOWN_ERROR.title(), NotificationType.UNKNOWN_ERROR.description(), e.getMessage()));
        }
    }

    //My code

    public String getState(String pipelineId, String pipelineElement){
        try {
            Pipeline pipeline = getPipelineStorage().getPipeline(pipelineId);
            return Operations.getState(pipeline, pipelineElement);
        } catch
        (Exception e) {
            e.printStackTrace();
            return "Failed in PipelineManagement" + e.getMessage();
        }
    }

    public String setState(String pipelineId, String pipelineElement, String state){
        try {
            Pipeline pipeline = getPipelineStorage().getPipeline(pipelineId);
            return Operations.setState(pipeline, pipelineElement, state);
        } catch
        (Exception e) {
            e.printStackTrace();
            return "Failed in PipelineManagement" + e.getMessage();
        }
    }

}
