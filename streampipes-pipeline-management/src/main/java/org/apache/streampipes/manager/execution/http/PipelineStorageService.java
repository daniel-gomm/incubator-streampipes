/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.manager.execution.http;

import org.apache.streampipes.manager.data.PipelineGraph;
import org.apache.streampipes.manager.data.PipelineGraphBuilder;
import org.apache.streampipes.manager.matching.InvocationGraphBuilder;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.Pipeline;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.graph.DataSinkInvocation;
import org.apache.streampipes.model.staticproperty.SecretStaticProperty;
import org.apache.streampipes.storage.management.StorageDispatcher;
import org.apache.streampipes.user.management.encryption.CredentialsManager;

import java.security.GeneralSecurityException;
import java.util.List;
import java.util.stream.Collectors;

public class PipelineStorageService {

    private Pipeline pipeline;

    public PipelineStorageService(Pipeline pipeline) {
        this.pipeline = pipeline;
    }

    public void updatePipeline() {
     preparePipeline();
     StorageDispatcher.INSTANCE.getNoSqlStore().getPipelineStorageAPI().updatePipeline(pipeline);
    }

    public void addPipeline() {
        preparePipeline();
        StorageDispatcher.INSTANCE.getNoSqlStore().getPipelineStorageAPI().store(pipeline);
    }

    private void preparePipeline() {
        PipelineGraph pipelineGraph = new PipelineGraphBuilder(pipeline).buildGraph();
        InvocationGraphBuilder builder = new InvocationGraphBuilder(pipelineGraph, pipeline.getPipelineId());
        List<InvocableStreamPipesEntity> graphs = encryptSecrets(builder.buildGraphs());

        List<DataSinkInvocation> secs = filter(graphs, DataSinkInvocation.class);
        List<DataProcessorInvocation> sepas = filter(graphs, DataProcessorInvocation.class);

        pipeline.setSepas(sepas);
        pipeline.setActions(secs);
    }

    private List<InvocableStreamPipesEntity> encryptSecrets(List<InvocableStreamPipesEntity> graphs) {
        graphs.forEach(g -> g.getStaticProperties()
                .stream()
                .filter(SecretStaticProperty.class::isInstance)
                .forEach(secret -> {
                    if (!((SecretStaticProperty) secret).getEncrypted()) {
                        try {
                            String encrypted = CredentialsManager.encrypt(pipeline.getCreatedByUser(),
                                    ((SecretStaticProperty) secret).getValue());
                            ((SecretStaticProperty) secret).setValue(encrypted);
                            ((SecretStaticProperty) secret).setEncrypted(true);
                        } catch (GeneralSecurityException e) {
                            e.printStackTrace();
                        }
                    }
                }));
        return graphs;
    }

    private <T> List<T> filter(List<InvocableStreamPipesEntity> graphs, Class<T> clazz) {
        return graphs
                .stream()
                .filter(clazz::isInstance)
                .map(clazz::cast)
                .collect(Collectors.toList());
    }
}
