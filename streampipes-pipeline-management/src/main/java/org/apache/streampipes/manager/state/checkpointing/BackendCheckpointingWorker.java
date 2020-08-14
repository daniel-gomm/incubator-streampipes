package org.apache.streampipes.manager.state.checkpointing;

import org.apache.streampipes.container.state.TrackedDatabase;
import org.apache.streampipes.manager.execution.http.HttpRequestBuilder;
import org.apache.streampipes.manager.state.rocksdb.BackendStateDatabase;
import org.apache.streampipes.manager.state.rocksdb.PipelineElementDatabase;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.PipelineElementStatus;

import java.util.Map;
import java.util.TreeMap;

public enum BackendCheckpointingWorker implements Runnable {
    INSTANCE;

    private static volatile TreeMap<Long, TrackedBackendDatabase> invocations= new TreeMap();
    private static volatile boolean isRunning = false;
    private static volatile BackendStateDatabase db;

    public void registerPipelineElement(InvocableStreamPipesEntity invoc, PipelineElementDatabase db){
        registerPipelineElement(invoc, db, 60000L);
    }

    public void registerPipelineElement(InvocableStreamPipesEntity invoc, PipelineElementDatabase db, Long interval){
        invocations.put(System.currentTimeMillis() + interval, new TrackedBackendDatabase(invoc, db, interval));
        if(!isRunning){
            this.startWorker();
        }
    }

    public void unregisterPipelineElement(String elementId){
        Map.Entry e = null;
        for(Map.Entry<Long, TrackedBackendDatabase> entry : invocations.entrySet()){
            if(elementId.equals(entry.getValue().elementID))
                e = entry;
        }
        if(e != null)
            invocations.remove(e.getKey(), e.getValue());

        if(invocations.isEmpty())
            INSTANCE.stopWorker();
    }

    public void updatePipelineElement(){
        //TODO
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

                        PipelineElementStatus resp = new HttpRequestBuilder(entry.getValue().invocableStreamPipesEntity, entry.getValue().invocableStreamPipesEntity.getUri() + "/checkpoint").getState();
                        if(resp.isSuccess()){
                            entry.getValue().db.add(resp.getOptionalMessage());
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

        }
    }
}
