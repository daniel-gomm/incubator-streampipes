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


package org.apache.streampipes.example.pe.processor.example;

import com.google.gson.Gson;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.model.PipelineElementState;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.runtime.EventProcessor;

import org.apache.streampipes.wrapper.runtime.StatefulEventProcessor;
import org.slf4j.Logger;

public class Example implements
        StatefulEventProcessor<ExampleParameters> {

  private static Logger LOG;

  private CounterState state = new CounterState();

  private class CounterState implements PipelineElementState {
    public int count = 0;
  }

  @Override
  public void onInvocation(ExampleParameters parameters,
        SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) throws SpRuntimeException {

  }

  @Override
  public void onEvent(Event event, SpOutputCollector out) throws SpRuntimeException {
    System.out.println(state.count);
    event.addField("counter", ++state.count);
    out.collect(event);
  }

  @Override
  public void onDetach() throws SpRuntimeException {

  }

  @Override
  public String getState() throws SpRuntimeException {
    Gson gson = new Gson();
    return gson.toJson(state);
  }

  @Override
  public void setState(String state) throws SpRuntimeException {
    Gson gson = new Gson();
    this.state = gson.fromJson(state, CounterState.class);
  }
}
