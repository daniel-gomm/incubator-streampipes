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
import org.apache.streampipes.model.State.PipelineElementState;
import org.apache.streampipes.model.graph.DataSinkInvocation;
import org.apache.streampipes.wrapper.context.EventSinkRuntimeContext;
import org.apache.streampipes.wrapper.params.binding.EventSinkBindingParams;
import org.apache.streampipes.wrapper.params.runtime.EventSinkRuntimeParams;
import org.apache.streampipes.wrapper.routing.SpInputCollector;
import org.apache.streampipes.wrapper.runtime.EventSink;
import org.apache.streampipes.wrapper.runtime.StatefulEventSink;
import org.apache.streampipes.wrapper.standalone.routing.StandaloneSpInputCollector;

import java.util.ArrayList;
import java.util.Map;
import java.util.function.Supplier;

public class StandaloneEventSinkRuntime<B extends EventSinkBindingParams> extends
        StandalonePipelineElementRuntime<B, DataSinkInvocation,
                EventSinkRuntimeParams<B>, EventSinkRuntimeContext, EventSink<B>> {

  public StandaloneEventSinkRuntime(Supplier<EventSink<B>> supplier, EventSinkRuntimeParams<B>
          params) {
    super(supplier, params);
  }

  @Override
  public void discardRuntime() throws SpRuntimeException {
    getInputCollectors().forEach(is -> is.unregisterConsumer(instanceId));
    discardEngine();
    postDiscard();
  }

  @Override
  public void process(Map rawEvent, String sourceInfo) throws SpRuntimeException {
    getEngine().onEvent(params.makeEvent(rawEvent, sourceInfo));
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

  }

  @Override
  public void postDiscard() throws SpRuntimeException {
    for(SpInputCollector spInputCollector : getInputCollectors()) {
      spInputCollector.disconnect();
    }
  }

  @Override
  public void bindEngine() throws SpRuntimeException {
    engine.onInvocation(params.getBindingParams(), params.getRuntimeContext());
  }

  @Override
  public void bindRuntime(PipelineElementState state) throws SpRuntimeException {
    bindEngine();
    if(engine instanceof StatefulEventSink){
      ((StatefulEventSink)engine).setElementId(this.params.getBindingParams().getGraph().getElementId());
      engine.onInvocation(params.getBindingParams(), params.getRuntimeContext());
      ((StatefulEventSink)engine).setState(state.state);
    }else{
      engine.onInvocation(params.getBindingParams(), params.getRuntimeContext());
    }
    getInputCollectors().forEach(is -> is.registerConsumer(instanceId, this));
    int i = 0;
    for (SpInputCollector spInputCollector : getInputCollectors()) {
      if (spInputCollector instanceof StandaloneSpInputCollector){
        StandaloneSpInputCollector inputCollector = (StandaloneSpInputCollector) spInputCollector;
        inputCollector.setConsumerState((String) state.consumerState.get(i++));
      }
    }
    prepareRuntime();
  }

  @Override
  public String getState() throws SpRuntimeException {
    PipelineElementState state = new PipelineElementState();
    state.consumerState = new ArrayList();
    boolean alreadyPaused = ((StandaloneSpInputCollector) getInputCollectors().get(0)).isPaused();
    if(!alreadyPaused){
      pause();
    }
    for (SpInputCollector spInputCollector : getInputCollectors()) {
      if (spInputCollector instanceof StandaloneSpInputCollector){
        state.consumerState.add(((StandaloneSpInputCollector) spInputCollector).getConsumerState());
      }
    }
    if(engine instanceof StatefulEventSink){
      state.state = ((StatefulEventSink)engine).getState();
    }
    if(!alreadyPaused){
      resume();
    }
    return new Gson().toJson(state);
  }

  @Override
  public void setState(String state) throws SpRuntimeException {
    PipelineElementState peState = new Gson().fromJson(state, PipelineElementState.class);
    int i = 0;
    for (SpInputCollector spInputCollector : getInputCollectors()){
      ((StandaloneSpInputCollector) spInputCollector).setConsumerState((String) peState.consumerState.get(i++));
    }
    if(engine instanceof StatefulEventSink){
      ((StatefulEventSink)engine).setState(peState.state);
    }
  }

  @Override
  public void pause() throws SpRuntimeException {
    for (SpInputCollector spInputCollector : getInputCollectors()) {
      if (spInputCollector instanceof StandaloneSpInputCollector){
        ((StandaloneSpInputCollector) spInputCollector).pauseConsumer();
      }
    }
  }

  @Override
  public void resume() throws SpRuntimeException {
    for (SpInputCollector spInputCollector : getInputCollectors()) {
      if (spInputCollector instanceof StandaloneSpInputCollector){
        ((StandaloneSpInputCollector) spInputCollector).resumeConsumer();
      }
    }
  }



}
