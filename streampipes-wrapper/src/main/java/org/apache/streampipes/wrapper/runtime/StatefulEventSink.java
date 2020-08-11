package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.container.state.StateHandler;
import org.apache.streampipes.wrapper.params.binding.EventSinkBindingParams;

public abstract class StatefulEventSink<B extends EventSinkBindingParams> implements EventSink<B>{
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
