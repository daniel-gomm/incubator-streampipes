package org.apache.streampipes.wrapper.declarer;

import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.sdk.extractor.AbstractParameterExtractor;
import org.apache.streampipes.wrapper.params.binding.BindingParams;
import org.apache.streampipes.wrapper.runtime.PipelineElementRuntime;

public abstract class StatefulPipelineElementDeclarer<B extends BindingParams, EPR extends
        PipelineElementRuntime, I
        extends InvocableStreamPipesEntity, EX extends AbstractParameterExtractor<I>> extends PipelineElementDeclarer<B,EPR,I,EX> {

    public Response getState(){
        return null;
    }

    public void setState(){
    }

}
