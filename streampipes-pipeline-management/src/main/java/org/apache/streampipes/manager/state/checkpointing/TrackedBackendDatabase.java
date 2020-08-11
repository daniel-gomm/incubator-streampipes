package org.apache.streampipes.manager.state.checkpointing;

import org.apache.streampipes.manager.state.rocksdb.BackendStateDatabase;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;

public class TrackedBackendDatabase {
    String elementID;
    InvocableStreamPipesEntity invocableStreamPipesEntity;
    Long interval;
    BackendStateDatabase db;
    public TrackedBackendDatabase(InvocableStreamPipesEntity invoc, BackendStateDatabase db, Long interval){
        this.elementID = invoc.getUri();
        this.invocableStreamPipesEntity = invoc;
        this.interval = interval;
        this.db = db;
    }
}
