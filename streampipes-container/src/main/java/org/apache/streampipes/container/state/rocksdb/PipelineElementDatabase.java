package org.apache.streampipes.container.state.rocksdb;

import org.apache.streampipes.container.state.CheckpointingWorker;
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
        this.columnFamilyHandle = StateDatabase.DATABASE.registerColumnFamily(this.columnFamily);
        this.lastAdded = StateDatabase.DATABASE.findLast(this.columnFamilyHandle);
    }

    public void add(String value){
        this.lastAdded = (this.columnFamily + "--" + System.currentTimeMillis()).getBytes();
        StateDatabase.DATABASE.save(this.lastAdded, value.getBytes(), this.columnFamilyHandle);
        StateDatabase.DATABASE.trim(this.columnFamilyHandle, this.retainedCheckpoints);
    }

    public String get(String key){
        return new String(StateDatabase.DATABASE.find(key.getBytes(), this.columnFamilyHandle));
    }

    public String getLast(){
        return new String(StateDatabase.DATABASE.find(this.lastAdded, this.columnFamilyHandle));
    }

    public void delete(String key){
        StateDatabase.DATABASE.delete(key.getBytes(), this.columnFamilyHandle);
    }

    public void close(){
        StateDatabase.DATABASE.closeFamily(this.columnFamilyHandle);
    }

}
