package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.state.PipelineElementState;

public abstract class StatefulPipelineElementRuntime extends PipelineElementRuntime {

    abstract public String getState() throws SpRuntimeException;

    abstract public void setState(String state) throws SpRuntimeException;

    abstract public void pause() throws SpRuntimeException;

    abstract public void resume() throws SpRuntimeException;

    abstract public String discardWithState() throws SpRuntimeException;

    abstract public void bindWithState(PipelineElementState state) throws SpRuntimeException;

}
