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

package org.apache.streampipes.wrapper.standalone.runtime;

import com.google.gson.Gson;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.state.PipelineElementState;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.wrapper.params.runtime.EventProcessorRuntimeParams;
import org.apache.streampipes.wrapper.routing.SpInputCollector;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.runtime.StatefulEventProcessor;
import org.apache.streampipes.wrapper.standalone.manager.ProtocolManager;
import org.apache.streampipes.wrapper.standalone.routing.StandaloneSpInputCollector;
import org.eclipse.rdf4j.query.algebra.Str;

import java.util.List;
import java.util.Map;
import java.util.function.Supplier;

public class StatefulStandaloneEventProcessorRuntime<B extends EventProcessorBindingParams> extends
        StatefulStandalonePipelineElementRuntime<B, DataProcessorInvocation,
                EventProcessorRuntimeParams<B>, EventProcessorRuntimeContext, StatefulEventProcessor<B>> {

    public StatefulStandaloneEventProcessorRuntime(Supplier<StatefulEventProcessor<B>> supplier,
                                           EventProcessorRuntimeParams<B> params) {
        super(supplier, params);
    }


    public SpOutputCollector getOutputCollector() throws SpRuntimeException {
        return ProtocolManager.findOutputCollector(params.getBindingParams().getGraph().getOutputStream()
                .getEventGrounding().getTransportProtocol(), params.getBindingParams().getGraph().getOutputStream
                ().getEventGrounding().getTransportFormats().get(0));
    }

    @Override
    public void discardRuntime() throws SpRuntimeException {
        getInputCollectors().forEach(is -> is.unregisterConsumer(instanceId));
        discardEngine();
        postDiscard();
    }

    @Override
    public void process(Map<String, Object> rawEvent, String sourceInfo) throws SpRuntimeException {
        getEngine().onEvent(params.makeEvent(rawEvent, sourceInfo), getOutputCollector());
    }

    @Override
    public void bindRuntime() throws SpRuntimeException {
        bindEngine();
        getInputCollectors().forEach(is -> is.registerConsumer(instanceId, this));
        prepareRuntime();
    }

    @Override
    public void prepareRuntime() throws SpRuntimeException {
        for (SpInputCollector spInputCollector : getInputCollectors()) {
            spInputCollector.connect();
        }

        getOutputCollector().connect();
    }

    @Override
    public void postDiscard() throws SpRuntimeException {
        for (SpInputCollector spInputCollector : getInputCollectors()) {
            spInputCollector.disconnect();
        }

        getOutputCollector().disconnect();
    }

    @Override
    public void bindEngine() throws SpRuntimeException {
        engine.onInvocation(params.getBindingParams(), getOutputCollector() , params.getRuntimeContext());
    }

    //My code

    public void bindWithState(String state) throws SpRuntimeException {
        Gson gson = new Gson();
        PipelineElementState elementState = gson.fromJson(state, PipelineElementState.class);
        bindEngine();
        setState(elementState.state);
        getInputCollectors().forEach(is -> is.registerConsumer(instanceId, this));
        int i = 0;
        for (SpInputCollector spInputCollector : getInputCollectors()) {
            if (spInputCollector.getClass().equals(StandaloneSpInputCollector.class)){
                StandaloneSpInputCollector inputCollector = (StandaloneSpInputCollector) spInputCollector;
                inputCollector.setConsumerState((String) elementState.consumerState.get(i++));
            }
        }
        prepareRuntime();
    }

    public String discardWithState() throws SpRuntimeException{
        PipelineElementState state = new PipelineElementState();
        state.state = engine.getState();
        discardEngine();
        for (SpInputCollector spInputCollector : getInputCollectors()) {
            if (spInputCollector.getClass().equals(StandaloneSpInputCollector.class)){
                state.consumerState.add(((StandaloneSpInputCollector) spInputCollector).getConsumerState(true));
            }
        }
        //Reihenfolge vertauscht (zurück tauschen?? sollte zu fehlern führen so wie es jetzt ist)
        getInputCollectors().forEach(is -> is.unregisterConsumer(instanceId));
        //postDiscard(); sollte nicht mehr nötig sein

        //Serialize state of pe
        Gson gson = new Gson();
        return gson.toJson(state);
    }

    public String getState() throws SpRuntimeException {
        String state = "";

        for (SpInputCollector spInputCollector : getInputCollectors()) {
            if (spInputCollector.getClass().equals(StandaloneSpInputCollector.class)){
                StandaloneSpInputCollector inputCollector = (StandaloneSpInputCollector) spInputCollector;
                state+=inputCollector.getConsumerState()+",\n";
            }
        }
        state = (state != "") ? state.substring(0, state.length()-2):state;
        System.out.println("State: " + state);
        return engine.getState();
    }

    public void setState(String state) throws SpRuntimeException {
        engine.setState(state);
    }

    //end of my code

}
