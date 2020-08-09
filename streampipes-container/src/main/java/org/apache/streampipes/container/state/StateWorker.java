package org.apache.streampipes.container.state;

import org.apache.streampipes.container.declarer.StatefulInvocableDeclarer;
import org.apache.streampipes.container.state.database.StateDB;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class StateWorker implements Runnable {

    private volatile Map<String, StatefulInvocableDeclarer> engines = new HashMap();
    private volatile boolean isRunning = true;
    private Long interval;

    public StateWorker(String key, StatefulInvocableDeclarer engine, Long interval){
        this.engines.put(key, engine);
        this.interval = interval;
        Thread thread = new Thread(this);
        thread.start();
    }

    public void stopWorker(){
        this.isRunning = false;
    }

    public void registerEngine(String key, StatefulInvocableDeclarer engine){
        this.engines.put(key, engine);
    }

    public void setInterval(long interval){
        this.interval = interval;
    }

    @Override
    public void run() {
        while (isRunning){
            try {
                Thread.sleep(interval);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
            for(Map.Entry<String, StatefulInvocableDeclarer> entry : engines.entrySet()){
                StateDB.addState(entry.getKey(), entry.getValue().getState().getOptionalMessage());
            }
        }
    }
}
