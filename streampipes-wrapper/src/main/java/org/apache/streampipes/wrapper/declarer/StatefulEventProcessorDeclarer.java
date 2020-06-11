package org.apache.streampipes.wrapper.declarer;

import org.apache.streampipes.container.declarer.StatefulInvocableDeclarer;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;
import org.apache.streampipes.wrapper.runtime.PipelineElementRuntime;

public abstract class StatefulEventProcessorDeclarer<B extends EventProcessorBindingParams, EPR extends
        PipelineElementRuntime> extends EventProcessorDeclarer<B, EPR> implements StatefulInvocableDeclarer<DataProcessorDescription, DataProcessorInvocation> {


}
