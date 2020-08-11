package org.apache.streampipes.pe.processor.counter;


import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class CounterParameters extends EventProcessorBindingParams {

  private String exampleText;

  public CounterParameters(DataProcessorInvocation graph, String exampleText) {
    super(graph);
    this.exampleText = exampleText;
  }

  public String getExampleText() {
    return exampleText;
  }

}
