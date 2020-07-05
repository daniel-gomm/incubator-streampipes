package org.apache.streampipes.container.declarer;

import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;

public interface StatefulInvocableDeclarer extends SemanticEventProcessingAgentDeclarer {

    Response getState();

    Response setState(String state);

    Response detachRuntimeAndGetState();

}
