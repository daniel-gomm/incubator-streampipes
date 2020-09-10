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

import org.apache.streampipes.manager.execution.http.HttpRequestBuilder;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.PipelineElementStatus;
import org.apache.streampipes.state.database.DatabasesSingleton;
import org.apache.streampipes.state.rocksdb.PipelineElementDatabase;
import org.apache.streampipes.state.rocksdb.StateDatabase;

import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;

public enum BackendCheckpointingWorker implements Runnable {
    INSTANCE;

    private static volatile TreeMap<Long, TrackedBackendDatabase> invocations= new TreeMap();
    private static volatile boolean isRunning = false;

    public void registerPipelineElement(InvocableStreamPipesEntity invoc){
        registerPipelineElement(invoc, DatabasesSingleton.INSTANCE.getDatabase(invoc.getElementId()), 30000L);
    }

    public void registerPipelineElement(InvocableStreamPipesEntity invoc, PipelineElementDatabase db, Long interval){
        invocations.put(System.currentTimeMillis() + interval, new TrackedBackendDatabase(invoc, db, interval));
        if(!isRunning){
            this.startWorker();
        }
    }

    public void unregisterPipelineElement(String elementId){
        for(Iterator<Map.Entry<Long, TrackedBackendDatabase>> iter = invocations.entrySet().iterator(); iter.hasNext();){
            Map.Entry<Long, TrackedBackendDatabase> entry = iter.next();
            if(elementId.equals(entry.getValue().elementID))
                iter.remove();
        }

        if(invocations.isEmpty())
            INSTANCE.stopWorker();
    }


    public void startWorker(){
        if(!isRunning){
            Thread t = new Thread(this);
            t.start();
            isRunning = true;
        }
    }

    public void stopWorker(){
        this.isRunning = false;
    }

    public static boolean isRunning(){
        return isRunning;
    }

    @Override
    public void run() {
        try{
            while(isRunning){
                Map.Entry<Long, TrackedBackendDatabase> entry = invocations.firstEntry();
                Long wait = Math.max(entry.getKey() - System.currentTimeMillis(), 0);
                try{
                    Thread.sleep(wait);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                //Exception handling in case that the PE is discarded while the checkpointing process is still ongoing
                try {
                    //Only start checkpointing if the PE is not unregistered yet
                    if (invocations.containsValue(entry.getValue())) {

                        PipelineElementStatus resp = new HttpRequestBuilder(entry.getValue().invocableStreamPipesEntity, entry.getValue().invocableStreamPipesEntity.getElementId() + "/checkpoint").getState();
                        if(resp.isSuccess()){
                            entry.getValue().db.add(resp.getOptionalMessage());
                        }else if(resp.getOptionalMessage() != null && resp.getOptionalMessage().startsWith(entry.getValue().elementID)){
                            //TODO Handle the case that the latest state has already been fetched (if it is not present, get it from the latest checkpoint id)
                        }else{
                            //TODO Handle the case that the state was unavailable (retry or what?)
                        }
                        //Update the key of the entry
                        invocations.remove(entry.getKey(), entry.getValue());
                        invocations.put(System.currentTimeMillis() + entry.getValue().interval, entry.getValue());
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
