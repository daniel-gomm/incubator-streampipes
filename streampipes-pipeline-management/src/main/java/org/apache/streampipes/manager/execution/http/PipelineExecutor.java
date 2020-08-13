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
import org.apache.streampipes.manager.state.checkpointing.BackendCheckpointingWorker;
import org.apache.streampipes.manager.state.rocksdb.BackendStateDatabase;
import org.apache.streampipes.model.State.PipelineElementState;
import org.lightcouch.DocumentConflictException;
import org.apache.streampipes.manager.execution.status.PipelineStatusManager;
import org.apache.streampipes.manager.execution.status.SepMonitoringManager;
import org.apache.streampipes.manager.util.TemporaryGraphStorage;
import org.apache.streampipes.model.SpDataSet;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.Pipeline;
import org.apache.streampipes.model.client.pipeline.PipelineOperationStatus;
import org.apache.streampipes.model.client.pipeline.PipelineStatusMessage;
import org.apache.streampipes.model.client.pipeline.PipelineStatusMessageType;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.graph.DataSinkInvocation;
import org.apache.streampipes.model.staticproperty.SecretStaticProperty;
import org.apache.streampipes.storage.api.IPipelineStorage;
import org.apache.streampipes.storage.management.StorageDispatcher;
import org.apache.streampipes.user.management.encryption.CredentialsManager;

import java.security.GeneralSecurityException;
import java.util.*;
import java.util.stream.Collectors;

public class PipelineExecutor {

  private Pipeline pipeline;
  private boolean visualize;
  private boolean storeStatus;
  private boolean monitor;

  public PipelineExecutor(Pipeline pipeline, boolean visualize, boolean storeStatus,
                          boolean monitor) {
    this.pipeline = pipeline;
    this.visualize = visualize;
    this.storeStatus = storeStatus;
    this.monitor = monitor;
  }

  public PipelineOperationStatus startPipeline() {

    List<DataProcessorInvocation> sepas = pipeline.getSepas();
    List<DataSinkInvocation> secs = pipeline.getActions();
    List<SpDataSet> dataSets = pipeline.getStreams().stream().filter(s -> s instanceof SpDataSet).map(s -> new
            SpDataSet((SpDataSet) s)).collect(Collectors.toList());

    for (SpDataSet ds : dataSets) {
      ds.setCorrespondingPipeline(pipeline.getPipelineId());
    }

    List<InvocableStreamPipesEntity> graphs = new ArrayList<>();
    graphs.addAll(sepas);
    graphs.addAll(secs);

    List<InvocableStreamPipesEntity> decryptedGraphs = decryptSecrets(graphs);

    graphs.forEach(g -> g.setStreamRequirements(Arrays.asList()));

    PipelineOperationStatus status = new GraphSubmitter(pipeline.getPipelineId(),
            pipeline.getName(), decryptedGraphs, dataSets)
            .invokeGraphs();

    if (status.isSuccess()) {
      storeInvocationGraphs(pipeline.getPipelineId(), graphs, dataSets);
      //graphs.forEach(g -> BackendCheckpointingWorker.INSTANCE.registerPipelineElement(g, new BackendStateDatabase(g.getUri())));
      PipelineStatusManager.addPipelineStatus(pipeline.getPipelineId(),
              new PipelineStatusMessage(pipeline.getPipelineId(), System.currentTimeMillis(), PipelineStatusMessageType.PIPELINE_STARTED.title(), PipelineStatusMessageType.PIPELINE_STARTED.description()));

      if (monitor) {
        SepMonitoringManager.addObserver(pipeline.getPipelineId());
      }

      if (storeStatus) {
        setPipelineStarted(pipeline);
      }
    }
    return status;
  }

  private List<InvocableStreamPipesEntity> decryptSecrets(List<InvocableStreamPipesEntity> graphs) {
    List<InvocableStreamPipesEntity> decryptedGraphs = new ArrayList<>();
    graphs.stream().map(g -> {
      if (g instanceof DataProcessorInvocation) {
        return new DataProcessorInvocation((DataProcessorInvocation) g);
      } else {
        return new DataSinkInvocation((DataSinkInvocation) g);
      }
    }).forEach(g -> {
      g.getStaticProperties()
              .stream()
              .filter(SecretStaticProperty.class::isInstance)
              .forEach(sp -> {
                try {
                  String decrypted = CredentialsManager.decrypt(pipeline.getCreatedByUser(),
                          ((SecretStaticProperty) sp).getValue());
                  ((SecretStaticProperty) sp).setValue(decrypted);
                  ((SecretStaticProperty) sp).setEncrypted(false);
                } catch (GeneralSecurityException e) {
                  e.printStackTrace();
                }
              });
      decryptedGraphs.add(g);
    });
    return decryptedGraphs;
  }

  public PipelineOperationStatus stopPipeline() {
    List<InvocableStreamPipesEntity> graphs = TemporaryGraphStorage.graphStorage.get(pipeline.getPipelineId());
    List<SpDataSet> dataSets = TemporaryGraphStorage.datasetStorage.get(pipeline.getPipelineId());

    PipelineOperationStatus status = new GraphSubmitter(pipeline.getPipelineId(),
            pipeline.getName(),  graphs, dataSets)
            .detachGraphs();

    if (status.isSuccess()) {
      if (visualize) {
        StorageDispatcher
                .INSTANCE
                .getNoSqlStore()
                .getVisualizationStorageApi()
                .deleteVisualization(pipeline.getPipelineId());
      }
      if (storeStatus) {
        setPipelineStopped(pipeline);
      }
      //graphs.forEach(g -> BackendCheckpointingWorker.INSTANCE.unregisterPipelineElement(g.getUri()));
      PipelineStatusManager.addPipelineStatus(pipeline.getPipelineId(),
              new PipelineStatusMessage(pipeline.getPipelineId(),
                      System.currentTimeMillis(),
                      PipelineStatusMessageType.PIPELINE_STOPPED.title(),
                      PipelineStatusMessageType.PIPELINE_STOPPED.description()));

      if (monitor) {
        SepMonitoringManager.removeObserver(pipeline.getPipelineId());
      }

    }
    return status;
  }

  private void setPipelineStarted(Pipeline pipeline) {
    pipeline.setRunning(true);
    pipeline.setStartedAt(new Date().getTime());
    try {
      getPipelineStorageApi().updatePipeline(pipeline);
    } catch (DocumentConflictException dce) {
      //dce.printStackTrace();
    }
  }

  private void setPipelineStopped(Pipeline pipeline) {
    pipeline.setRunning(false);
    getPipelineStorageApi().updatePipeline(pipeline);
  }

  private void storeInvocationGraphs(String pipelineId, List<InvocableStreamPipesEntity> graphs,
                                     List<SpDataSet> dataSets) {
    TemporaryGraphStorage.graphStorage.put(pipelineId, graphs);
    TemporaryGraphStorage.datasetStorage.put(pipelineId, dataSets);
  }

  private IPipelineStorage getPipelineStorageApi() {
    return StorageDispatcher.INSTANCE.getNoSqlStore().getPipelineStorageAPI();
  }

  public PipelineOperationStatus migrate(String elementList){
    List<InvocableStreamPipesEntity> graphs = TemporaryGraphStorage.graphStorage.get(pipeline.getPipelineId());
    List<SpDataSet> dataSets = TemporaryGraphStorage.datasetStorage.get(pipeline.getPipelineId());

    Gson gson = new Gson();

    Element[] elements = gson.fromJson(elementList, Element[].class);

    ArrayList<InvocableStreamPipesEntity> onlyMigrate = new ArrayList<InvocableStreamPipesEntity>();
    //get List of PEs to migrate
    for (InvocableStreamPipesEntity sepa : graphs){
      for (Element ele : elements){
        if (sepa.getElementId().equals(ele.oldElement)){
          onlyMigrate.add(sepa);
        };
      }
    }


    //pause Pipeline Elements that should be migrated
    PipelineOperationStatus status = new GraphSubmitter(pipeline.getPipelineId(),
            pipeline.getName(), onlyMigrate, dataSets).pause();
    if(!status.isSuccess()){
      //handle unsuccessful pause
      status.setTitle("Could not pause the Pipeline Elements.");
      return status;
    }

    //Get states of these pipeline Elements
    Map<String, PipelineElementState> states = new GraphSubmitter(pipeline.getPipelineId(),
            pipeline.getName(), onlyMigrate, dataSets).getStates();


    //try to start PEs as new Elements
    //Change Pipeline Description
    for (DataProcessorInvocation sepa : this.pipeline.getSepas()){
      for(Element ele : elements) {
        if (sepa.getElementId().equals(ele.oldElement)) {
          sepa.setElementId(ele.newElement);
          sepa.setBelongsTo(ele.newElement.substring(0, ele.newElement.lastIndexOf("/")));
        };
      }
    }
    for(InvocableStreamPipesEntity g : onlyMigrate){
      for(Element ele :elements){
        if(g.getElementId().equals(ele.oldElement)){
          g.setElementId(ele.newElement);
          g.setBelongsTo(ele.newElement.substring(0, ele.newElement.lastIndexOf("/")));
        }
      }
    }
    TemporaryGraphStorage.graphStorage.put(pipeline.getPipelineId(), graphs);

    //Start PE with received states
    List<InvocableStreamPipesEntity> decryptedGraphs = decryptSecrets(onlyMigrate);

    onlyMigrate.forEach(g -> g.setStreamRequirements(Arrays.asList()));

    PipelineOperationStatus statusInvoc = new GraphSubmitter(pipeline.getPipelineId(),
            pipeline.getName(), decryptedGraphs, new ArrayList<SpDataSet>())
            .invokeGraphs(states);


    //handle the migration outcome
    if(statusInvoc.isSuccess()){
      //Stop/discard old Elements
      //TODO enhance later
      for(InvocableStreamPipesEntity g : onlyMigrate){
        for(Element ele :elements){
          if(g.getElementId().equals(ele.newElement)){
            g.setElementId(ele.oldElement);
            g.setBelongsTo(ele.oldElement.substring(0, ele.oldElement.lastIndexOf("/")));
          }
        }
      }
      PipelineOperationStatus detach = new GraphSubmitter(pipeline.getPipelineId(),
              pipeline.getName(), onlyMigrate, dataSets).detachGraphs();
      for(InvocableStreamPipesEntity g : onlyMigrate){
        for(Element ele :elements){
          if(g.getElementId().equals(ele.oldElement)){
            g.setElementId(ele.newElement);
            g.setBelongsTo(ele.newElement.substring(0, ele.newElement.lastIndexOf("/")));
          }
        }
      }
      if(detach.isSuccess()){
        storeInvocationGraphs(pipeline.getPipelineId(), graphs, dataSets);
        PipelineStatusManager.addPipelineStatus(pipeline.getPipelineId(),
                new PipelineStatusMessage(pipeline.getPipelineId(), System.currentTimeMillis(), PipelineStatusMessageType.PIPELINE_STARTED.title(), PipelineStatusMessageType.PIPELINE_STARTED.description()));
      } else{
        statusInvoc.setTitle("Failed to detach old Elements.");
        statusInvoc.setSuccess(false);
      }
      return statusInvoc;
    }else{
      //Change back the description
      for (DataProcessorInvocation sepa : this.pipeline.getSepas()){
        for(Element ele : elements) {
          if (sepa.getElementId().equals(ele.newElement)) {
            sepa.setElementId(ele.oldElement);
            sepa.setBelongsTo(ele.oldElement.substring(0, ele.oldElement.lastIndexOf("/")));
          };
        }
        for(InvocableStreamPipesEntity g : onlyMigrate){
          for(Element ele :elements){
            if(g.getElementId().equals(ele.newElement)){
              g.setElementId(ele.oldElement);
              g.setBelongsTo(ele.oldElement.substring(0, ele.oldElement.lastIndexOf("/")));
            }
          }
        }
        TemporaryGraphStorage.graphStorage.put(pipeline.getPipelineId(), graphs);
      }
      //Resume processing
      PipelineOperationStatus statusResume = new GraphSubmitter(pipeline.getPipelineId(),
              pipeline.getName(), onlyMigrate, dataSets).resume();
      if(statusResume.isSuccess()){
        statusResume.setSuccess(false);
        statusResume.setTitle("Resumed on old Elements, migration unsuccessful.");
      }//TODO else
      return statusResume;
    }
  }

  private class Element{
    String oldElement;
    String newElement;
  }

}
