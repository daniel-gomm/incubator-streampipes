package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.container.state.StateHandler;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public abstract class StatefulEventProcessor <B extends EventProcessorBindingParams> implements StatefulPipelineElement<B, DataProcessorInvocation>, EventProcessor<B>{

    protected StateHandler stateHandler;
    protected String elementId;


    @Override
    public String getState(){
        return this.stateHandler.getState();
    }

    @Override
    public void setState(String state){
        this.stateHandler.setState(state);
    }

    public void setElementId(String elementId){
        this.elementId = elementId;
    }
}
