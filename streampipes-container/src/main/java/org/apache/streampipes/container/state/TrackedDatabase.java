package org.apache.streampipes.container.state;

import org.apache.streampipes.container.declarer.InvocableDeclarer;
import org.apache.streampipes.container.state.rocksdb.StateDatabase;

public class TrackedDatabase {
    StateDatabase db;
    InvocableDeclarer invocableDeclarer;
    Long waitInterval;
    String elementId;

    public TrackedDatabase(StateDatabase db, Long waitInterval, InvocableDeclarer invocableDeclarer, String elementId){
        this.db = db;
        this.waitInterval = waitInterval;
        this.invocableDeclarer = invocableDeclarer;
        this.elementId = elementId;
    }
}
