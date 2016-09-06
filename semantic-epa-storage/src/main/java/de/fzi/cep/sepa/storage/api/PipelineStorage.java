package de.fzi.cep.sepa.storage.api;

import java.util.List;

import de.fzi.cep.sepa.model.client.pipeline.Pipeline;
import de.fzi.cep.sepa.model.client.VirtualSensor;

public interface PipelineStorage {

	List<Pipeline> getAllPipelines();

	List<Pipeline> getAllUserPipelines();

	void storePipeline(Pipeline pipeline);

	void updatePipeline(Pipeline pipeline);

	Pipeline getPipeline(String pipelineId);

	void deletePipeline(String pipelineId);

	void store(Pipeline object);

	void storeVirtualSensor(String username, VirtualSensor virtualSensor);

	List<VirtualSensor> getVirtualSensors(String username);
}
