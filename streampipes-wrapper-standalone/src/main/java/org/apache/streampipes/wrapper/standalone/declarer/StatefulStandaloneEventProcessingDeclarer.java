package org.apache.streampipes.wrapper.standalone.declarer;

import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.wrapper.declarer.StatefulEventProcessorDeclarer;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.wrapper.params.runtime.EventProcessorRuntimeParams;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.runtime.StandaloneEventProcessorRuntime;

public abstract class StatefulStandaloneEventProcessingDeclarer<B extends
        EventProcessorBindingParams> extends StatefulEventProcessorDeclarer<B, StandaloneEventProcessorRuntime> {

    public abstract ConfiguredEventProcessor<B> onInvocation(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor);

    @Override
    public StandaloneEventProcessorRuntime<B> getRuntime(DataProcessorInvocation graph,
                                                         ProcessingElementParameterExtractor extractor) {
        ConfiguredEventProcessor<B> configuredEngine = onInvocation(graph, extractor);
        EventProcessorRuntimeParams<B> runtimeParams = new EventProcessorRuntimeParams<>
                (configuredEngine.getBindingParams(), false);

        return new StandaloneEventProcessorRuntime<>(configuredEngine.getEngineSupplier(),
                runtimeParams);
    }

}
