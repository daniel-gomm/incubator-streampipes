package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.wrapper.params.binding.BindingParams;
import org.apache.streampipes.wrapper.state.StateHandler;

public interface StatefulPipelineElement<B extends BindingParams<I>, I extends
        InvocableStreamPipesEntity> extends PipelineElement<B, I> {

    String getState() throws SpRuntimeException;

    void setState(String state) throws SpRuntimeException;


}
