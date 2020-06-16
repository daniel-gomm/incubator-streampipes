package org.apache.streampipes.wrapper.declarer;

import org.apache.streampipes.container.declarer.StatefulInvocableDeclarer;
import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.wrapper.runtime.StatefulPipelineElementRuntime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class StatefulEventProcessorDeclarer<B extends EventProcessorBindingParams, EPR extends
        StatefulPipelineElementRuntime> extends StatefulPipelineElementDeclarer<B, EPR, DataProcessorInvocation,
        ProcessingElementParameterExtractor> implements StatefulInvocableDeclarer {

    public static final Logger logger = LoggerFactory.getLogger(EventProcessorDeclarer.class.getCanonicalName());

    @Override
    protected ProcessingElementParameterExtractor getExtractor(DataProcessorInvocation graph) {
        return ProcessingElementParameterExtractor.from(graph);
    }

    @Override
    public Response invokeRuntime(DataProcessorInvocation graph) {
        return invokeEPRuntime(graph);
    }


}
