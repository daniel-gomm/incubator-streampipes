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

package org.apache.streampipes.examples.pe.processor.aggregator;

import org.apache.commons.lang.StringUtils;
import org.apache.streampipes.commons.evaluation.EvaluationLogger;
import org.apache.streampipes.state.handling.StateHandler;
import org.apache.streampipes.state.annotations.StateObject;
import org.apache.streampipes.wrapper.runtime.StatefulEventProcessor;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;

import org.slf4j.Logger;

import java.util.LinkedList;
import java.util.Queue;
import java.util.Stack;

public class Aggregator extends
        StatefulEventProcessor<AggregatorParameters> {

  private static Logger LOG;
  
  @StateObject public Queue<Double> pastEvents = new LinkedList<>();
  @StateObject public double value = 0;
  @StateObject public String blowUpDinero;
  private int window_size;
  private int state_size;


  @Override
  public void onInvocation(AggregatorParameters parameters,
                           SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
    this.stateHandler = new StateHandler(this);
    this.window_size = parameters.getWindow_size();
    this.state_size = parameters.getStateSize();
  }

  @Override
  public void onEvent(Event event, SpOutputCollector out) {
    if(blowUpDinero == null){
      blowUpDinero = StringUtils.repeat("DDDDDDDD", state_size*1024/16);
    }
    double currentValue = event.getFieldByRuntimeName("value").getAsPrimitive().getAsDouble();
    if(pastEvents.size() < this.window_size){
      pastEvents.offer(currentValue);
      value += currentValue;
    }else if (pastEvents.size() == this.window_size){
      pastEvents.offer(currentValue);
      value = value + currentValue - pastEvents.poll();
    }else{
      System.out.println("Queue longer than " + this.window_size);
    }
    event.addField("aggregation", value);
    System.out.println("processing:" + currentValue + " | result: " + value);
    out.collect(event);
    EvaluationLogger.log("eventProcessed", System.currentTimeMillis(), currentValue);
  }

  @Override
  public void onDetach() {
  }
}
