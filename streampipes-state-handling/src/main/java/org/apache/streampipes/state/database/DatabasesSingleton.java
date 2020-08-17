package org.apache.streampipes.state.database;

import org.apache.streampipes.state.rocksdb.PipelineElementDatabase;

import java.util.HashMap;

public enum DatabasesSingleton {
    INSTANCE;

    private HashMap<String, PipelineElementDatabase> databasesMap  = new HashMap<>();

    public void addNew(String elementId){
        addDatabase(elementId, new PipelineElementDatabase(elementId));
    }

    public void addDatabase(String elementId, PipelineElementDatabase db){
        if(!databasesMap.containsKey(elementId)){
            this.databasesMap.put(elementId, db);
        }
    }

    public PipelineElementDatabase getDatabase(String elementId){
        return this.databasesMap.get(elementId);
    }
}
