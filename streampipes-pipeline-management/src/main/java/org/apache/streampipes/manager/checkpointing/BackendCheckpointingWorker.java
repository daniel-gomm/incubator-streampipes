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

package org.apache.streampipes.manager.checkpointing;

import org.apache.streampipes.commons.evaluation.EvaluationLogger;
import org.apache.streampipes.manager.execution.http.HttpRequestBuilder;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.PipelineElementStatus;
import org.apache.streampipes.state.checkpointing.CheckpointingWorker;

import java.util.*;

public enum BackendCheckpointingWorker implements CheckpointingWorker {
    INSTANCE;

    private static final TreeMap<Long, BackendTrackedDatabase> invocations= new TreeMap<>();
    private static volatile boolean isRunning = false;
    private static final List<String> trackedElements = new LinkedList<>();
    private Thread thread;


    public void registerPipelineElement(InvocableStreamPipesEntity invoc){
        registerPipelineElement(invoc, 30000L);
    }

    public void registerPipelineElement(InvocableStreamPipesEntity invoc, Long interval){
        synchronized (invocations){
            trackedElements.add(invoc.getElementId());
            boolean inserted = false;
            long key = System.currentTimeMillis() + 5000; //Time added for evaluation ( + interval;)
            while (!inserted){
                if(invocations.containsKey(key)){
                    key++;
                } else{
                    invocations.put(key, new BackendTrackedDatabase(invoc, interval));
                    inserted = true;
                }
            }
            System.out.println("Registered " + invoc.getElementId());
            EvaluationLogger.log("CheckpointingWorker", "Registered " + invoc.getElementId(), System.currentTimeMillis());
            if(!isRunning) this.startWorker();
        }
    }

    public void unregisterPipelineElement(String elementId){
        synchronized (invocations){
            trackedElements.remove(elementId);
            invocations.entrySet().removeIf(entry -> elementId.equals(entry.getValue().elementID));
            if(invocations.isEmpty())
                INSTANCE.stopWorker();
            System.out.println("Unregistered " + elementId);
            EvaluationLogger.log("CheckpointingWorker", "Unregistered " + elementId, System.currentTimeMillis());
        }

    }


    public void startWorker(){
        isRunning = true;
        if(this.thread == null || !this.thread.isAlive()){
            this.thread = new Thread(this);
            this.thread.start();
        }
    }

    public void stopWorker(){
        this.isRunning = false;
    }


    @Override
    public void run() {
        try{
            while(isRunning){
                Map.Entry<Long, BackendTrackedDatabase> entry = invocations.firstEntry();
                Long wait = Math.max(entry.getKey() - System.currentTimeMillis(), 0);
                try{
                    Thread.sleep(wait);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                if(!trackedElements.contains(entry.getValue().elementID)){
                    unregisterPipelineElement(entry.getValue().elementID);
                    continue;
                }
                //Exception handling in case that the PE is discarded while the checkpointing process is still ongoing
                try {
                    //Only start checkpointing if the PE is not unregistered yet
                    //if (invocations.containsValue(entry.getValue())) {
                    synchronized (invocations){
                        if (trackedElements.contains(entry.getValue().elementID)) {
                            PipelineElementStatus resp = new HttpRequestBuilder(entry.getValue().invocableStreamPipesEntity, entry.getValue().invocableStreamPipesEntity.getElementId() + "/checkpoint").getState();
                            if(resp.isSuccess()){
                                entry.getValue().db.add(resp.getOptionalMessage());
                                System.out.println("Got state: " + entry.getValue().elementID);
                                EvaluationLogger.log("CheckpointingWorker", "Got state: " + entry.getValue().elementID, System.currentTimeMillis());
                            }else if(resp.getOptionalMessage() != null && resp.getOptionalMessage().startsWith(entry.getValue().elementID)){
                                //The latest state has already been fetched (if it is not present, get it from the latest checkpoint id)
                                System.out.println("Latest state already fetched.");
                            }else{
                                //The state was unavailable
                                System.out.println("State unavailable\n" + resp.getOptionalMessage());
                            }
                            //Update the key of the entry
                            invocations.entrySet().removeIf(e -> e.getValue() == entry.getValue());
                            invocations.remove(entry.getKey());
                            boolean inserted = false;
                            long key = System.currentTimeMillis() + entry.getValue().interval;
                            while (!inserted){
                                if(invocations.containsKey(key)){
                                    key++;
                                } else{
                                    invocations.put(key, entry.getValue());
                                    inserted = true;
                                }
                            }
                        }
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch (Exception e){
            e.printStackTrace();
        }finally {
            isRunning = false;
        }
    }
}
