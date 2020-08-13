package org.apache.streampipes.examples.pe.processor.example;

import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.EpRequirements;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.helpers.SupportedFormats;
import org.apache.streampipes.sdk.helpers.SupportedProtocols;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class ExampleController extends StandaloneEventProcessingDeclarer<ExampleParameters> {

	private static final String EXAMPLE_KEY = "example-key";

	@Override
	public DataProcessorDescription declareModel() {
		return ProcessingElementBuilder.create("org.apache.streampipes.examples.pe.processor.example")
				.withAssets(Assets.DOCUMENTATION, Assets.ICON)
				.withLocales(Locales.EN)
				.category(DataProcessorType.AGGREGATE)
				.requiredStream(StreamRequirementsBuilder
						.create()
						.requiredProperty(EpRequirements.anyProperty())
						.build())
				.requiredTextParameter(Labels.withId(EXAMPLE_KEY))
				.supportedFormats(SupportedFormats.jsonFormat())
				.supportedProtocols(SupportedProtocols.kafka())
				.outputStrategy(OutputStrategies.append(EpProperties.integerEp(Labels.from("cnt","Count of events","Reflects how many events have been processed."), "cnt", SO.Number)))
				.build();
	}

	@Override
	public ConfiguredEventProcessor<ExampleParameters> onInvocation
				(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

		String exampleString = extractor.singleValueParameter(EXAMPLE_KEY, String.class);

		ExampleParameters params = new ExampleParameters(graph, exampleString);

		return new ConfiguredEventProcessor<>(params, Example::new);
	}

}
