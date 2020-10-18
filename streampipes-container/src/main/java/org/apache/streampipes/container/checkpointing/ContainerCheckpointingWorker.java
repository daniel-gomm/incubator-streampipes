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

package org.apache.streampipes.container.checkpointing;

import org.apache.streampipes.container.declarer.InvocableDeclarer;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;
import org.apache.streampipes.state.checkpointing.CheckpointingWorker;
import org.apache.streampipes.state.database.DatabasesSingleton;


import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

public enum ContainerCheckpointingWorker implements CheckpointingWorker {
    INSTANCE;


    private static TreeMap<Long, ContainerTrackedDatabase> invocations= new TreeMap<>();
    private static volatile boolean isRunning = false;
    private static final List<String> trackedElements = new LinkedList<>();
    private Thread thread;

    public static void registerPipelineElement(InvocableDeclarer<NamedStreamPipesEntity, InvocableStreamPipesEntity> invocation, String elementId){
        registerPipelineElement(invocation, 3000L, elementId);
    }

    public static void registerPipelineElement(InvocableDeclarer<NamedStreamPipesEntity, InvocableStreamPipesEntity> invocation, Long interval, String elementId){
        synchronized (invocations){
            //System.out.println("Registered: " + elementId);
            trackedElements.add(elementId);

            boolean inserted = false;
            long key = System.currentTimeMillis() + interval;
            while (!inserted){
                if(invocations.containsKey(key)){
                    key++;
                } else{
                    invocations.put(key, new
                            ContainerTrackedDatabase(DatabasesSingleton.INSTANCE.getDatabase(elementId),
                            interval, invocation, elementId));
                    inserted = true;
                }
            }
            if(!isRunning){
                INSTANCE.startWorker();
            }
        }
    }

    public static void unregisterPipelineElement(String runningInstanceId){
        synchronized (invocations){
            //System.out.println("Unregistered: " + runningInstanceId);
            trackedElements.remove(runningInstanceId);
            invocations.entrySet().removeIf(entry -> runningInstanceId.equals(entry.getValue().elementId));
            if(invocations.isEmpty())
                INSTANCE.stopWorker();
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
        isRunning = false;
    }


    @Override
    public void run() {
        try{
            while(isRunning){
                //System.out.println("BF:" + invocations.toString() + "Thread:" + Thread.currentThread().getName());
                Map.Entry<Long, ContainerTrackedDatabase> entry = invocations.firstEntry();
                //System.out.println(invocations.toString() + entry.toString());
                long wait = Math.max(entry.getKey() - System.currentTimeMillis(), 0);
                try{
                    Thread.sleep(wait);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                if(!trackedElements.contains(entry.getValue().elementId)){
                    unregisterPipelineElement(entry.getValue().elementId);
                    continue;
                }
                //Exception handling in case that the PE is discarded while the checkpointing process is still ongoing
                try {
                    //Only start checkpointing if the PE is not unregistered yet
                    //if (invocations.containsValue(entry.getValue())) {
                    synchronized (invocations){
                        //System.out.println(state);
                        entry.getValue().db.add(entry.getValue().invocableDeclarer.getState());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
                //Update the key of the entry
                if(invocations.containsKey(entry.getKey())){
                    invocations.entrySet().removeIf(e -> e.getValue() == entry.getValue());
                    invocations.remove(entry.getKey());
                    boolean inserted = false;
                    long key = (System.currentTimeMillis() + entry.getValue().waitInterval);
                    while (!inserted){
                        if(invocations.containsKey(key) && !invocations.containsValue(entry.getValue())){
                            key++;
                        } if(invocations.containsValue(entry.getValue())) break;
                        else{
                            invocations.put(key, entry.getValue());
                            inserted = true;
                        }
                    }
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            isRunning = false;
        }
    }
}
