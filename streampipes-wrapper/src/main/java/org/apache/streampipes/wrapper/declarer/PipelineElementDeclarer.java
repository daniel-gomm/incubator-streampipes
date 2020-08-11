/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.wrapper.declarer;

import org.apache.streampipes.container.state.rocksdb.StateDatabase;
import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.State.PipelineElementState;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.sdk.extractor.AbstractParameterExtractor;
import org.apache.streampipes.wrapper.params.binding.BindingParams;
import org.apache.streampipes.wrapper.runtime.PipelineElementRuntime;

public abstract class PipelineElementDeclarer<B extends BindingParams, EPR extends
        PipelineElementRuntime, I
        extends InvocableStreamPipesEntity, EX extends AbstractParameterExtractor<I>> {

  protected EPR epRuntime;
  protected String elementId;
  protected StateDatabase db;

  //TODO find expression for NodeID, e.g. graph.getAppId()
  public Response invokeEPRuntime(I graph) {
    try {
      elementId = graph.getElementId();
      db = new StateDatabase(elementId, "NodeID");
      // change to getRuntime(graph, extractor)
      epRuntime = getRuntime(graph, getExtractor(graph));
      epRuntime.bindRuntime();
      return new Response(graph.getElementId(), true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Response(graph.getElementId(), false, e.getMessage());
    }

  }

  public Response detachRuntime(String pipelineId) {
    try {
      epRuntime.discardRuntime();
      return new Response(elementId, true);
    } catch (Exception e) {
      e.printStackTrace();
      return new Response(elementId, false, e.getMessage());
    }
  }

  protected abstract EX getExtractor(I graph);

  public abstract EPR getRuntime(I graph, EX extractor);



  public Response invokeEPRuntime(I graph, PipelineElementState state){
    try{
      elementId = graph.getElementId();
      // change to getRuntime(graph, extractor)
      epRuntime = getRuntime(graph, getExtractor(graph));
      epRuntime.bindRuntime(state);
      return new Response(elementId, true, "Successfully initiated with state");
    }
    catch (Exception e){
      e.printStackTrace();
      return new Response(elementId, false, e.getMessage());
    }
  }

  public StateDatabase getDatabase(){
    return this.db;
  }

  public String getState(){
    try {
      return epRuntime.getState();
    } catch (Exception e){
      e.printStackTrace();
      return null;
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

  public Response pause(){
    try{
      epRuntime.pause();
      return new Response(elementId, true);
    }catch(Exception e){
      e.printStackTrace();
      return new Response(elementId, false, e.getMessage());
    }
  }

  public Response resume(){
    try{
      epRuntime.resume();
      return new Response(elementId, true);
    }catch(Exception e){
      e.printStackTrace();
      return new Response(elementId, false, e.getMessage());
    }
  }

}
