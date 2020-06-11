package org.apache.streampipes.wrapper.standalone.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.model.SpDataStream;
import org.apache.streampipes.model.base.InvocableStreamPipesEntity;
import org.apache.streampipes.wrapper.context.RuntimeContext;
import org.apache.streampipes.wrapper.params.binding.BindingParams;
import org.apache.streampipes.wrapper.params.runtime.RuntimeParams;
import org.apache.streampipes.wrapper.routing.RawDataProcessor;
import org.apache.streampipes.wrapper.routing.SpInputCollector;
import org.apache.streampipes.wrapper.runtime.StatefulPipelineElement;
import org.apache.streampipes.wrapper.runtime.StatefulPipelineElementRuntime;
import org.apache.streampipes.wrapper.standalone.manager.ProtocolManager;

import java.util.ArrayList;
import java.util.List;
import java.util.function.Supplier;

public abstract class StatefulStandalonePipelineElementRuntime<B extends BindingParams<I>,
        I extends InvocableStreamPipesEntity,
        RP extends RuntimeParams<B, I, RC>,
        RC extends RuntimeContext,
        P extends StatefulPipelineElement<B, I>>
        extends StatefulPipelineElementRuntime implements RawDataProcessor {

    protected RP params;
    protected final P engine;

    public StatefulStandalonePipelineElementRuntime(Supplier<P> supplier, RP runtimeParams) {
        super();
        this.engine = supplier.get();
        this.params = runtimeParams;
    }

    public P getEngine() {
        return engine;
    }

    public void discardEngine() throws SpRuntimeException {
        engine.onDetach();
    }

    public List<SpInputCollector> getInputCollectors() throws SpRuntimeException {
        List<SpInputCollector> inputCollectors = new ArrayList<>();
        for (SpDataStream is : params.getBindingParams().getGraph().getInputStreams()) {
            inputCollectors.add(ProtocolManager.findInputCollector(is.getEventGrounding()
                            .getTransportProtocol(), is.getEventGrounding().getTransportFormats().get(0),
                    params.isSingletonEngine()));
        }
        return inputCollectors;
    }

    public abstract void bindEngine() throws SpRuntimeException;


    //My code

    public String getState() throws SpRuntimeException{
        return engine.getState();
    }

    public void setState(String state) throws SpRuntimeException{
        engine.setState(state);
    }

    //End of my code

}
