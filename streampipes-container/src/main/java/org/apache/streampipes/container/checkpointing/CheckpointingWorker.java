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
import org.apache.streampipes.state.database.DatabasesSingleton;
import org.apache.streampipes.state.rocksdb.StateDatabase;

import java.util.Map;
import java.util.TreeMap;

public enum CheckpointingWorker implements Runnable{
    INSTANCE;


    private static volatile TreeMap<Long, TrackedDatabase> invocations= new TreeMap();
    private static volatile boolean isRunning = false;

    public static void registerPipelineElement(InvocableDeclarer invocation, String elementId){
        registerPipelineElement(invocation, 3000L, elementId);
    }

    public static void registerPipelineElement(InvocableDeclarer invocation, Long interval, String elementId){
        invocations.put(System.currentTimeMillis() + interval,
                new TrackedDatabase(DatabasesSingleton.INSTANCE.getDatabase(elementId),
                        interval, invocation,
                        elementId.split("/")[elementId.split("/").length - 1]));
        if(!isRunning()){
            INSTANCE.startWorker();
        }
    }

    public static void unregisterPipelineElement(String runningInstanceId){
        Map.Entry e = null;
        for(Map.Entry<Long, TrackedDatabase> entry : invocations.entrySet()){
            if(runningInstanceId.equals(entry.getValue().elementId))
                e = entry;
        }
        if(e != null)
            invocations.remove(e.getKey(), e.getValue());

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
                Map.Entry<Long, TrackedDatabase> entry = invocations.firstEntry();
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
                        String state = entry.getValue().invocableDeclarer.getState();
                        System.out.println(state);
                        entry.getValue().db.add(state);
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
                //Update the key of the entry
                invocations.remove(entry.getKey(), entry.getValue());
                invocations.put(System.currentTimeMillis() + entry.getValue().waitInterval, entry.getValue());
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            isRunning = false;
        }
    }
}
