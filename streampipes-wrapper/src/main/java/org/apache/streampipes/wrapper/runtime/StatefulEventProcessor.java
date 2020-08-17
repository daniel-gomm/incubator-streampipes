package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.state.handling.StateHandler;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public abstract class StatefulEventProcessor <B extends EventProcessorBindingParams> implements EventProcessor<B> {

    protected StateHandler stateHandler;
    protected String elementId;


    public String getState(){
        return this.stateHandler.getState();
    }

    public void setState(String state){
        this.stateHandler.setState(state);
    }
    //TODO assess usefulness and necessity
    public void setElementId(String elementId){
        this.elementId = elementId;
    }
}
