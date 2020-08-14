package org.apache.streampipes.manager.state.rocksdb;

import org.apache.streampipes.container.state.rocksdb.StateDatabase;
import org.rocksdb.ColumnFamilyHandle;

public class PipelineElementDatabase {
    private final String columnFamily;
    private ColumnFamilyHandle columnFamilyHandle = null;
    private byte[] lastAdded;
    private Long retainedCheckpoints;

    public PipelineElementDatabase(String elementId) {
        this(elementId, 50L);
    }

    public PipelineElementDatabase(String elementId, Long retainedCheckpoints) {
        this.columnFamily = elementId;
        this.retainedCheckpoints = retainedCheckpoints;
        this.columnFamilyHandle = BackendStateDatabase.DATABASE.registerPipelineElement(this.columnFamily);
        this.lastAdded = BackendStateDatabase.DATABASE.findLast(this.columnFamilyHandle);
    }

    public void add(String value){
        this.lastAdded = (this.columnFamily + "--" + System.currentTimeMillis()).getBytes();
        BackendStateDatabase.DATABASE.save(this.lastAdded, value.getBytes(), this.columnFamilyHandle);
        BackendStateDatabase.DATABASE.trim(this.columnFamilyHandle, this.retainedCheckpoints);
    }

    public String get(String key){
        return new String(BackendStateDatabase.DATABASE.find(key.getBytes(), this.columnFamilyHandle));
    }

    public String getLast(){
        return new String(BackendStateDatabase.DATABASE.find(this.lastAdded, this.columnFamilyHandle));
    }

    public void delete(String key){
        BackendStateDatabase.DATABASE.delete(key.getBytes(), this.columnFamilyHandle);
    }

    public void close(){
        BackendStateDatabase.DATABASE.closeFamily(this.columnFamilyHandle);
    }

}
