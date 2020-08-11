package org.apache.streampipes.container.state;

import org.apache.streampipes.container.declarer.InvocableDeclarer;
import org.apache.streampipes.container.state.rocksdb.StateDatabase;

import java.util.Map;
import java.util.TreeMap;

public class CheckpointingWorker implements Runnable{


    private static volatile TreeMap<Long, TrackedDatabase> invocations= new TreeMap();
    private static volatile boolean isRunning = true;

    public static void registerPipelineElement(InvocableDeclarer invocation, String elementId){
        registerPipelineElement(invocation, 10000L, elementId);
    }

    public static void registerPipelineElement(InvocableDeclarer invocation, Long interval, String elementId){
        invocations.put(System.currentTimeMillis() + interval, new TrackedDatabase(invocation.getDatabase(), interval, invocation, elementId));
    }

    public static void unregisterPipelineElement(String elementId){
        for(Map.Entry<Long, TrackedDatabase> entry : invocations.entrySet()){
            if(elementId.equals(entry.getValue().elementId)){
                invocations.remove(entry.getValue());
            }
        }
    }

    public void startWorker(){
        if(!isRunning){
            Thread t = new Thread(this);
            t.start();
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
                Long wait = Math.min(entry.getKey() - System.currentTimeMillis(), 0);
                try{
                    Thread.sleep(wait);
                }catch (InterruptedException e){
                    e.printStackTrace();
                }
                //Exception handling in case that the PE is discarded while the checkpointing process is still ongoing
                try {
                    //Only start checkpointing if the PE is not unregistered yet
                    if (invocations.containsValue(entry.getValue())) {
                        String key = entry.getValue().elementId + System.currentTimeMillis();
                        entry.getValue().db.save(key, entry.getValue().invocableDeclarer.getState());
                        //Update the key of the entry
                        invocations.remove(entry);
                        invocations.put(System.currentTimeMillis() + entry.getValue().waitInterval, entry.getValue());
                    }
                }catch(Exception e){
                    e.printStackTrace();
                }
            }
        }catch(Exception e){
            e.printStackTrace();
        }finally {
            //Manage the closure of the open databases
            for(Map.Entry<Long, TrackedDatabase> entry : invocations.entrySet()){
                entry.getValue().db.closeFamily();
            }
            StateDatabase.close();
            isRunning = false;
        }
    }
}
