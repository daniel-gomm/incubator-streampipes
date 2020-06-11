package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;

public interface StatefulEventProcessor <B extends EventProcessorBindingParams> extends StatefulPipelineElement<B, DataProcessorInvocation>{

    void onInvocation(B parameters, SpOutputCollector spOutputCollector, EventProcessorRuntimeContext runtimeContext) throws
            SpRuntimeException;

    void onEvent(Event event, SpOutputCollector collector) throws SpRuntimeException;

}
