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

package de.fzi.cep.sepa.html.page;

import org.apache.streampipes.container.state.rocksdb.StateDatabase;
import org.apache.streampipes.model.State.PipelineElementState;
import org.junit.Test;
import org.apache.streampipes.container.declarer.DataStreamDeclarer;
import org.apache.streampipes.container.declarer.SemanticEventProcessingAgentDeclarer;
import org.apache.streampipes.container.declarer.SemanticEventProducerDeclarer;
import org.apache.streampipes.container.html.model.DataSourceDescriptionHtml;
import org.apache.streampipes.container.html.model.Description;
import org.apache.streampipes.container.html.page.WelcomePageGenerator;
import org.apache.streampipes.container.html.page.WelcomePageGeneratorImpl;
import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.SpDataStream;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.model.graph.DataSourceDescription;

import java.net.URI;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.hamcrest.core.IsInstanceOf.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

public class WelcomePageGeneratorImplTest {

    @Test
    public void buildUrisWithEmptyListTest() {
        WelcomePageGenerator wpg = new WelcomePageGeneratorImpl("baseUri", new ArrayList<>());
        List<Description> actual = wpg.buildUris();

        assertEquals(actual.size(), 0);
    }

    @Test
    public void buildUrisWithSepaTest() {
        WelcomePageGenerator wpg = new WelcomePageGeneratorImpl("baseUri/", Arrays.asList(getSepaDeclarer()));
        List<Description> actual = wpg.buildUris();
        Description expected = new Description("sepaname", "sepadescription", URI.create("baseUri/sepa/sepapathName"));

        assertEquals(1, actual.size());
        assertEquals(expected, actual.get(0));
    }

    @Test
    public void buildUrisWithSepTest() {
        WelcomePageGenerator wpg = new WelcomePageGeneratorImpl("baseUri/", Arrays.asList(getSepdDeclarer()));
        List<Description> actual = wpg.buildUris();
        Description expected = new Description("sepname", "sepdescription", URI.create("baseUri/sep/seppathName"));

        assertEquals(actual.size(), 1);
        Description desc = actual.get(0);
        assertEquals(expected.getName(), desc.getName());
        assertEquals(expected.getDescription(), desc.getDescription());
        assertEquals(expected.getUri(), desc.getUri());

        assertThat(desc,  instanceOf(DataSourceDescriptionHtml.class));

        DataSourceDescriptionHtml sepDesc = (DataSourceDescriptionHtml) desc;
        assertEquals(1, sepDesc.getStreams().size());
        Description expectedStream = new Description("streamname", "streamdescription", URI.create("baseUri/stream/streampathName"));

        assertEquals(expectedStream, sepDesc.getStreams().get(0));
    }

    private SemanticEventProcessingAgentDeclarer getSepaDeclarer() {
        return new SemanticEventProcessingAgentDeclarer() {
            @Override
            public Response invokeRuntime(DataProcessorInvocation invocationGraph) {
                return null;
            }

            @Override
            public Response detachRuntime(String pipelineId) {
                return null;
            }

            @Override
            public Response invokeRuntime(DataProcessorInvocation invocationGraph, PipelineElementState state) {
                return null;
            }

            @Override
            public String getState() {
                return null;
            }

            @Override
            public Response setState(String state) {
                return null;
            }

            @Override
            public Response pause() {
                return null;
            }

            @Override
            public Response resume() {
                return null;
            }

            @Override
            public DataProcessorDescription declareModel() {
                return new DataProcessorDescription("sepapathName", "sepaname", "sepadescription", "iconUrl");
            }

            @Override
            public StateDatabase getDatabase() {
                return null;
            }
        };
    }

    private SemanticEventProducerDeclarer getSepdDeclarer() {
        return new SemanticEventProducerDeclarer() {
            @Override
            public List<DataStreamDeclarer> getEventStreams() {
                return Arrays.asList(new DataStreamDeclarer() {
                    @Override
                    public SpDataStream declareModel(DataSourceDescription sep) {
                        return new SpDataStream("streampathName", "streamname", "streamdescription", null);
                    }

                    @Override
                    public void executeStream() {

                    }

                    @Override
                    public boolean isExecutable() {
                        return false;
                    }
                });
            }

            @Override
            public DataSourceDescription declareModel() {
                return new DataSourceDescription("seppathName", "sepname", "sepdescription", "sepiconUrl");
            }
        };
    }

}