package org.apache.streampipes.examples.pe.processor.example;

import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class ExampleParameters extends EventProcessorBindingParams {

  private String exampleText;

  public ExampleParameters(DataProcessorInvocation graph, String exampleText) {
    super(graph);
    this.exampleText = exampleText;
  }

  public String getExampleText() {
    return exampleText;
  }

}
