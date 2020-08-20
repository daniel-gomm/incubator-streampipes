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

package org.apache.streampipes.container.api;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.streampipes.container.checkpointing.CheckpointingWorker;
import org.apache.streampipes.model.state.StatefulPayload;
import org.apache.streampipes.state.database.DatabasesSingleton;
import org.eclipse.rdf4j.repository.RepositoryException;
import org.eclipse.rdf4j.rio.RDFParseException;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.container.declarer.Declarer;
import org.apache.streampipes.container.declarer.InvocableDeclarer;
import org.apache.streampipes.container.init.RunningInstances;
import org.apache.streampipes.container.transform.Transformer;
import org.apache.streampipes.container.util.Util;
import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.runtime.RuntimeOptionsRequest;
import org.apache.streampipes.model.runtime.RuntimeOptionsResponse;
import org.apache.streampipes.model.staticproperty.Option;
import org.apache.streampipes.sdk.extractor.AbstractParameterExtractor;
import org.apache.streampipes.sdk.extractor.StaticPropertyExtractor;
import org.apache.streampipes.serializers.json.GsonSerializer;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

public abstract class InvocableElement<I extends InvocableStreamPipesEntity, D extends Declarer,
        P extends AbstractParameterExtractor<I>> extends Element<D> {

    protected abstract Map<String, D> getElementDeclarers();
    protected abstract String getInstanceId(String uri, String elementId);

    protected Class<I> clazz;

    public InvocableElement(Class<I> clazz) {
        this.clazz = clazz;
    }

    @POST
    @Path("{elementId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String invokeRuntime(@PathParam("elementId") String elementId, String payload) {

        //My code
        String originalPayload = payload;
        StatefulPayload load = null;
        try {
            Gson gson = new Gson();
            load = gson.fromJson(payload, StatefulPayload.class);
            payload = load.namedStreamPipesEntity;
            if(payload == null){
                payload = originalPayload;
            }
        }catch (JsonSyntaxException e){
            e.printStackTrace();
            payload = originalPayload;
        }

        //End of my code

        try {
            I graph = Transformer.fromJsonLd(clazz, payload);

            if (isDebug()) {
                graph = createGroundingDebugInformation(graph);
            }

            InvocableDeclarer declarer = (InvocableDeclarer) getDeclarerById(elementId);

            if (declarer != null) {
                String runningInstanceId = getInstanceId(graph.getElementId(), elementId);
                RunningInstances.INSTANCE.add(runningInstanceId, graph, declarer.getClass().newInstance());
                //My code
                Response resp;
                if (load.pipelineElementState == null) {
                    resp = RunningInstances.INSTANCE.getInvocation(runningInstanceId).invokeRuntime(graph);
                }
                else{
                    resp = RunningInstances.INSTANCE.getInvocation(runningInstanceId).invokeRuntime(graph, load.pipelineElementState);
                }
                RunningInstances.INSTANCE.registerForCheckpointing(runningInstanceId, graph.getElementId());
                //End of my code
                return Util.toResponseString(resp);
            }
        } catch (RDFParseException | IOException | RepositoryException | InstantiationException | IllegalAccessException e) {
            e.printStackTrace();
            return Util.toResponseString(new Response(elementId, false, e.getMessage()));
        }

        return Util.toResponseString(elementId, false, "Could not find the element with id: " + elementId);

    }

    @POST
    @Path("{elementId}/configurations")
    public String fetchConfigurations(@PathParam("elementId") String elementId, String payload) {

        RuntimeOptionsRequest runtimeOptionsRequest = GsonSerializer.getGsonWithIds().fromJson(payload,
                RuntimeOptionsRequest.class);
        ResolvesContainerProvidedOptions resolvesOptions = (ResolvesContainerProvidedOptions) getDeclarerById(elementId);

        List<Option> availableOptions =
                resolvesOptions.resolveOptions(runtimeOptionsRequest.getRequestId(),
                StaticPropertyExtractor.from(
                        runtimeOptionsRequest.getStaticProperties(),
                        runtimeOptionsRequest.getInputStreams(),
                        runtimeOptionsRequest.getAppId()
                ));

        return GsonSerializer.getGsonWithIds().toJson(new RuntimeOptionsResponse(runtimeOptionsRequest,
                availableOptions));
    }

    @POST
    @Path("{elementId}/output")
    public String fetchOutputStrategy(@PathParam("elementId") String elementId, String payload) {

        I runtimeOptionsRequest = GsonSerializer.getGsonWithIds().fromJson(payload, clazz);
        ResolvesContainerProvidedOutputStrategy<I, P> resolvesOutput =
                (ResolvesContainerProvidedOutputStrategy<I, P>)
                getDeclarerById
                (elementId);

        try {
            return GsonSerializer.getGsonWithIds().toJson(resolvesOutput.resolveOutputStrategy
                    (runtimeOptionsRequest, getExtractor(runtimeOptionsRequest)));
        } catch (SpRuntimeException e) {
            e.printStackTrace();
            return Util.toResponseString(runtimeOptionsRequest.getElementId(), false);
        }
    }


    // TODO move endpoint to /elementId/instances/runningInstanceId
    @DELETE
    @Path("{elementId}/{runningInstanceId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String detach(@PathParam("elementId") String elementId, @PathParam("runningInstanceId") String runningInstanceId) {

        InvocableDeclarer runningInstance = RunningInstances.INSTANCE.getInvocation(runningInstanceId);

        if (runningInstance != null) {
            Response resp = runningInstance.detachRuntime(runningInstanceId);

            if (resp.isSuccess()) {
                RunningInstances.INSTANCE.remove(runningInstanceId);
            }

            return Util.toResponseString(resp);
        }

        return Util.toResponseString(elementId, false, "Could not find the running instance with id: " + runningInstanceId);
    }

    protected abstract P getExtractor(I graph);

    protected abstract I createGroundingDebugInformation(I graph);

    private Boolean isDebug() {
        return "true".equals(System.getenv("SP_DEBUG"));
    }

    @GET
    @Path("{elementId}/{runningInstanceId}/state")
    @Produces(MediaType.APPLICATION_JSON)
    public String getState(@PathParam("elementId") String elementId, @PathParam("runningInstanceId") String runningInstanceId){

        InvocableDeclarer runningInstance = RunningInstances.INSTANCE.getInvocation(runningInstanceId);

        if (runningInstance != null) {
            Response resp = new Response(elementId, true, runningInstance.getState());
            return Util.toResponseString(resp);
        }
        return Util.toResponseString(elementId, false, "Could not find the running instance with id: " + runningInstanceId);

    }

    @PUT
    @Path("{elementId}/{runningInstanceId}/state")
    @Consumes(MediaType.APPLICATION_JSON)
    public String setState(@PathParam("elementId") String elementId, @PathParam("runningInstanceId") String runningInstanceId, String payload){
        InvocableDeclarer runningInstance = RunningInstances.INSTANCE.getInvocation(runningInstanceId);
        if (runningInstance != null) {
            Response resp = runningInstance.setState(payload);

            return Util.toResponseString(resp);
        }
        return Util.toResponseString(elementId, false, "Could not find the running instance with id: " + runningInstanceId);
    }


    @GET
    @Path("{elementId}/{runningInstanceId}/pause")
    @Produces(MediaType.APPLICATION_JSON)
    public String pauseElement(@PathParam("elementId") String elementId, @PathParam("runningInstanceId") String runningInstanceId){
        InvocableDeclarer runningInstance = RunningInstances.INSTANCE.getInvocation(runningInstanceId);
        if (runningInstance == null){
            return Util.toResponseString(elementId, false, "Could not find the running stateful instance with id: " + runningInstanceId);
        }
        return Util.toResponseString(runningInstance.pause());
    }

    @GET
    @Path("{elementId}/{runningInstanceId}/resume")
    @Produces(MediaType.APPLICATION_JSON)
    public String resumeElement(@PathParam("elementId") String elementId, @PathParam("runningInstanceId") String runningInstanceId){
        InvocableDeclarer runningInstance = RunningInstances.INSTANCE.getInvocation(runningInstanceId);
        if (runningInstance == null){
            return Util.toResponseString(elementId, false, "Could not find the running stateful instance with id: " + runningInstanceId);
        }
        return Util.toResponseString(runningInstance.resume());
    }

    @GET
    @Path("{elementId}/{runningInstanceId}/checkpoint")
    @Produces(MediaType.APPLICATION_JSON)
    public String getCheckpoint(@PathParam("elementId") String elementId, @PathParam("runningInstanceId") String runningInstanceId){
        try {
            String checkpoint = DatabasesSingleton.INSTANCE.getDatabase(runningInstanceId).getLast();

            if (checkpoint != null) {
                Response resp = new Response(elementId, true, checkpoint);
                return Util.toResponseString(resp);
            } else {
                return Util.toResponseString(elementId, false, DatabasesSingleton.INSTANCE.getDatabase(runningInstanceId).getLastKey());
            }
        }catch (Exception e){
            return Util.toResponseString(elementId, false, e.getMessage());
        }

    }


}

