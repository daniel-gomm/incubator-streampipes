package org.apache.streampipes.wrapper.declarer;

import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.sdk.extractor.AbstractParameterExtractor;
import org.apache.streampipes.wrapper.params.binding.BindingParams;
import org.apache.streampipes.wrapper.runtime.StatefulPipelineElementRuntime;

public abstract class StatefulPipelineElementDeclarer<B extends BindingParams, EPR extends
        StatefulPipelineElementRuntime, I
        extends InvocableStreamPipesEntity, EX extends AbstractParameterExtractor<I>> extends PipelineElementDeclarer<B,EPR,I,EX> {

    public Response getState(){
        try{
            return new Response(elementId, true, epRuntime.getState());
        }catch(Exception e){
            e.printStackTrace();
            return new Response(elementId, false, e.getMessage());
        }
    }

    public Response setState(String state){
        try{
            epRuntime.setState(state);
            return new Response(elementId, true);
        }catch(Exception e){
            e.printStackTrace();
            return new Response(elementId, false, e.getMessage());
        }
    }

    public Response detachRuntimeAndGetState(){
        try{
            return new Response(elementId, true, epRuntime.discardWithState());
        }catch(Exception e){
            e.printStackTrace();
            return new Response(elementId, false, e.getMessage());
        }
    }

}
