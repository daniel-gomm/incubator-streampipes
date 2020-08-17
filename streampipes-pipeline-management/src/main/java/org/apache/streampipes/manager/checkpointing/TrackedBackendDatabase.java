package org.apache.streampipes.manager.checkpointing;

import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.state.database.DatabasesSingleton;
import org.apache.streampipes.state.rocksdb.PipelineElementDatabase;

public class TrackedBackendDatabase {
    String elementID;
    InvocableStreamPipesEntity invocableStreamPipesEntity;
    Long interval;
    PipelineElementDatabase db;
    public TrackedBackendDatabase(InvocableStreamPipesEntity invoc, PipelineElementDatabase db, Long interval){
        this.elementID = invoc.getElementId();
        this.invocableStreamPipesEntity = invoc;
        this.interval = interval;
        this.db = db;
        this.db = DatabasesSingleton.INSTANCE.getDatabase(invoc.getElementId());
    }
}
