package org.apache.streampipes.wrapper.runtime;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public interface StatefulEventProcessor <B extends EventProcessorBindingParams> extends EventProcessor<B>{

    String getState() throws SpRuntimeException;

    void setState(String state) throws SpRuntimeException;

}
