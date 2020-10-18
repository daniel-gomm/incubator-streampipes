package org.apache.streampipes.state.checkpointing;

public interface CheckpointingWorker extends Runnable{

    void startWorker();

    void stopWorker();


}
