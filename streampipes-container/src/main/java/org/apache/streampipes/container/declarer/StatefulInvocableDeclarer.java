package org.apache.streampipes.container.declarer;

import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;

public interface StatefulInvocableDeclarer<D extends NamedStreamPipesEntity, I extends InvocableStreamPipesEntity> extends InvocableDeclarer<D, I> {

    Response getState();

    Response setState(String state);

}
