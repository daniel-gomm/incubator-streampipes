package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.runtime.Event;
import org.apache.streampipes.wrapper.context.EventProcessorRuntimeContext;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.wrapper.routing.SpOutputCollector;
import org.apache.streampipes.wrapper.state.StateHandler;

public abstract class StatefulEventProcessor <B extends EventProcessorBindingParams> implements StatefulPipelineElement<B, DataProcessorInvocation>, EventProcessor<B>{
    /**
    protected StateHandler stateHandler;


    @Override
    public String getState() throws SpRuntimeException {
        return this.stateHandler.getState();
    }

    @Override
    public void setState(String state) throws SpRuntimeException {
        this.stateHandler.setState(state);
    }
    **/
}
