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

package org.apache.streampipes.manager.execution.http;

import com.google.gson.Gson;
import org.apache.streampipes.model.state.PipelineElementState;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.streampipes.model.SpDataSet;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.PipelineElementStatus;
import org.apache.streampipes.model.client.pipeline.PipelineOperationStatus;

import java.util.*;

public class GraphSubmitter {

  private List<InvocableStreamPipesEntity> graphs;
  private List<SpDataSet> dataSets;

  private String pipelineId;
  private String pipelineName;

  private final static Logger LOG = LoggerFactory.getLogger(GraphSubmitter.class);

  public GraphSubmitter(String pipelineId, String pipelineName, List<InvocableStreamPipesEntity> graphs,
                        List<SpDataSet> dataSets) {
    this.graphs = graphs;
    this.pipelineId = pipelineId;
    this.pipelineName = pipelineName;
    this.dataSets = dataSets;
  }

  public PipelineOperationStatus invokeGraphs() {
    PipelineOperationStatus status = new PipelineOperationStatus();
    status.setPipelineId(pipelineId);
    status.setPipelineName(pipelineName);


    graphs.forEach(g -> status.addPipelineElementStatus(new HttpRequestBuilder(g, g.getBelongsTo()).invoke()));
    if (status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess)) {
      dataSets.forEach(dataSet ->
              status.addPipelineElementStatus
                      (new HttpRequestBuilder(dataSet, dataSet.getUri()).invoke()));
    }
    status.setSuccess(status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess));

    if (status.isSuccess()) {
      status.setTitle("Pipeline " + pipelineName + " successfully started");
    } else {
      LOG.info("Could not start pipeline, initializing rollback...");
      rollbackInvokedPipelineElements(status);
      status.setTitle("Could not start pipeline" + pipelineName + ".");
    }
    return status;
  }

  private void rollbackInvokedPipelineElements(PipelineOperationStatus status) {
    for (PipelineElementStatus s : status.getElementStatus()) {
      if (s.isSuccess()) {
        Optional<InvocableStreamPipesEntity> graph = findGraph(s.getElementId());
        graph.ifPresent(g -> {
          LOG.info("Rolling back element " + g.getElementId());
          new HttpRequestBuilder(g, g.getBelongsTo()).detach();
        });
      }
    }
  }

  private Optional<InvocableStreamPipesEntity> findGraph(String elementId) {
    return graphs.stream().filter(g -> g.getBelongsTo().equals(elementId)).findFirst();
  }

  public PipelineOperationStatus detachGraphs() {
    PipelineOperationStatus status = new PipelineOperationStatus();
    status.setPipelineId(pipelineId);
    status.setPipelineName(pipelineName);

    graphs.forEach(g -> status.addPipelineElementStatus(new HttpRequestBuilder(g, g.getUri()).detach()));
    dataSets.forEach(dataSet -> status.addPipelineElementStatus(new HttpRequestBuilder(dataSet, dataSet.getUri() +
            "/" +dataSet.getDatasetInvocationId())
            .detach()));
    status.setSuccess(status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess));
    if (status.isSuccess()) {
      status.setTitle("Pipeline " + pipelineName + " successfully stopped");
    } else {
      status.setTitle("Could not stop all pipeline elements of pipeline " + pipelineName + ".");
    }

    return status;
  }

  public PipelineOperationStatus invokeGraphs(Map<String, PipelineElementState> states) {
    PipelineOperationStatus status = new PipelineOperationStatus();
    status.setPipelineId(pipelineId);
    status.setPipelineName(pipelineName);


    for (Iterator<InvocableStreamPipesEntity> iter = graphs.iterator(); iter.hasNext(); ) {
      InvocableStreamPipesEntity spe = iter.next();
      PipelineElementStatus resp = null;
      if(states.containsKey(spe.getElementId().split("/")[spe.getElementId().split("/").length-1])){
        resp = new HttpRequestBuilder(spe, spe.getBelongsTo()).invoke(states.get(spe.getElementId().split("/")[spe.getElementId().split("/").length-1]));
      } else{
        //If no state is availible start regularly
        resp = new HttpRequestBuilder(spe, spe.getBelongsTo()).invoke();
      }
      status.addPipelineElementStatus(resp);
    }

    if (status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess)) {
      dataSets.forEach(dataSet ->
              status.addPipelineElementStatus
                      (new HttpRequestBuilder(dataSet, dataSet.getUri()).invoke()));
    }
    status.setSuccess(status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess));

    if (status.isSuccess()) {
      status.setTitle("Pipeline " + pipelineName + " successfully started");
    } else {
      LOG.info("Could not start pipeline, initializing rollback...");
      rollbackInvokedPipelineElements(status);
      status.setTitle("Could not start pipeline" + pipelineName + ".");
    }
    return status;
  }

  public Map<String, PipelineElementState> getStates(){
    Map<String, PipelineElementState> states = new HashMap<String, PipelineElementState>();
    Gson gson = new Gson();

    PipelineOperationStatus status = new PipelineOperationStatus();
    status.setPipelineId(pipelineId);
    status.setPipelineName(pipelineName);

    for (Iterator<InvocableStreamPipesEntity> iter = graphs.iterator(); iter.hasNext(); ){
      InvocableStreamPipesEntity spe = iter.next();
      PipelineElementStatus resp = new HttpRequestBuilder(spe, spe.getUri() + "/state").getState();
      status.addPipelineElementStatus(resp);
      if (resp.isSuccess()) {
        states.put(spe.getElementId().split("/")[spe.getElementId().split("/").length-1], gson.fromJson(resp.getOptionalMessage(), PipelineElementState.class));
      }
      status.setSuccess(status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess));

    }


    if (status.isSuccess()) {
      status.setTitle("State of " + pipelineName + " successfully obtained.");
    } else {
      status.setTitle("Could not get all states for " + pipelineName + ".");
    }

    return states;
  }


  public PipelineOperationStatus pause(){
    PipelineOperationStatus status = new PipelineOperationStatus();
    status.setPipelineId(pipelineId);
    status.setPipelineName(pipelineName);


    for (Iterator<InvocableStreamPipesEntity> iter = graphs.iterator(); iter.hasNext(); ) {
      InvocableStreamPipesEntity spe = iter.next();
      PipelineElementStatus resp = new HttpRequestBuilder(spe, spe.getUri()).pause();
      status.addPipelineElementStatus(resp);
    }

    status.setSuccess(status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess));

    if (status.isSuccess()) {
      status.setTitle("Pipeline " + pipelineName + " successfully paused");
    } else {
      LOG.info("Could not pause pipeline.");
      status.setTitle("Could not pause pipeline" + pipelineName + ".");
    }
    return status;
  }


  public PipelineOperationStatus resume(){
    PipelineOperationStatus status = new PipelineOperationStatus();
    status.setPipelineId(pipelineId);
    status.setPipelineName(pipelineName);


    for (Iterator<InvocableStreamPipesEntity> iter = graphs.iterator(); iter.hasNext(); ) {
      InvocableStreamPipesEntity spe = iter.next();
      PipelineElementStatus resp = new HttpRequestBuilder(spe, spe.getUri()).resume();
      status.addPipelineElementStatus(resp);
    }

    status.setSuccess(status.getElementStatus().stream().allMatch(PipelineElementStatus::isSuccess));

    if (status.isSuccess()) {
      status.setTitle("Pipeline " + pipelineName + " successfully paused");
    } else {
      LOG.info("Could not pause pipeline.");
      status.setTitle("Could not pause pipeline" + pipelineName + ".");
    }
    return status;
  }

}
