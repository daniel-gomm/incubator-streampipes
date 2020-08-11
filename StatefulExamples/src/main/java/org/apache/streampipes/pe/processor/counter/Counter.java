
package org.apache.streampipes.pe.processor.counter;

import org.apache.streampipes.container.state.StateHandler;
import org.apache.streampipes.container.state.annotations.StateObject;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;

import org.apache.streampipes.wrapper.runtime.StatefulEventProcessor;
import org.slf4j.Logger;

public class Counter extends StatefulEventProcessor<CounterParameters> {

  private static Logger LOG;

  @StateObject public int counter = 0;

  public Counter(){
    super();
  }

  @Override
  public void onInvocation(CounterParameters parameters,
                           SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) {
    this.stateHandler = new StateHandler(this);
  }

  @Override
  public void onEvent(Event event, SpOutputCollector out) {
    System.out.println(counter);
    event.addField("counter", ++counter);
    out.collect(event);
  }

  @Override
  public void onDetach() {

  }
}
