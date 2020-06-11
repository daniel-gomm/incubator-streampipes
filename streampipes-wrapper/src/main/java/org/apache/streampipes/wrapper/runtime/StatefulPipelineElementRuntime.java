package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;

public abstract class StatefulPipelineElementRuntime extends PipelineElementRuntime {

    abstract public String getState() throws SpRuntimeException;

    abstract public void setState(String state) throws SpRuntimeException;

}
