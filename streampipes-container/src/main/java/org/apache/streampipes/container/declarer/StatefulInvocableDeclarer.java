package org.apache.streampipes.container.declarer;

import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.state.PipelineElementState;

public interface StatefulInvocableDeclarer extends SemanticEventProcessingAgentDeclarer {

    Response getState();

    Response setState(String state);

    Response detachRuntimeAndGetState();

    Response invokeStatefulRuntime(DataProcessorInvocation graph, PipelineElementState state);

}
