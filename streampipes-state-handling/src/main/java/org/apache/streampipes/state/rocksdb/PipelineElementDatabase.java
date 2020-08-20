package org.apache.streampipes.state.rocksdb;

import org.apache.streampipes.state.database.DatabasesSingleton;
import org.rocksdb.ColumnFamilyHandle;

public class PipelineElementDatabase {

    private final String columnFamily;
    private ColumnFamilyHandle columnFamilyHandle = null;
    private byte[] lastAdded;
    private Long retainedCheckpoints;
    private boolean returnedLatest = false;

    public PipelineElementDatabase(String elementId) {
        this(elementId, 15L);
    }

    public PipelineElementDatabase(String elementId, Long retainedCheckpoints) {
        this.columnFamily = elementId;
        this.retainedCheckpoints = retainedCheckpoints;
        this.columnFamilyHandle = StateDatabase.DATABASE.registerColumnFamily(this.columnFamily);
        this.lastAdded = StateDatabase.DATABASE.findLast(this.columnFamilyHandle);
        DatabasesSingleton.INSTANCE.addDatabase(elementId, this);
    }

    public PipelineElementDatabase(String elementId, ColumnFamilyHandle cfHandle){
        this.columnFamily = elementId;
        this.columnFamilyHandle = cfHandle;
        this.retainedCheckpoints = 15L;
        this.lastAdded = StateDatabase.DATABASE.findLast(cfHandle);
        DatabasesSingleton.INSTANCE.addDatabase(elementId, this);
    }

    public void add(String value){
        returnedLatest = false;
        this.lastAdded = (this.columnFamily + "--" + System.currentTimeMillis()).getBytes();
        StateDatabase.DATABASE.save(this.lastAdded, value.getBytes(), this.columnFamilyHandle);
        StateDatabase.DATABASE.trim(this.columnFamilyHandle, this.retainedCheckpoints);
    }

    public String get(String key){
        return new String(StateDatabase.DATABASE.find(key.getBytes(), this.columnFamilyHandle));
    }

    public String getLast(){
        if(this.returnedLatest){
            return null;
        }else{
            this.returnedLatest = true;
            return new String(StateDatabase.DATABASE.find(this.lastAdded, this.columnFamilyHandle));
        }
    }

    public String getLastKey(){
        return new String(this.lastAdded);
    }

    public void delete(String key){
        StateDatabase.DATABASE.delete(key.getBytes(), this.columnFamilyHandle);
    }

    public void delete(){
        StateDatabase.DATABASE.deleteFamily(this.columnFamilyHandle);
    }

}
