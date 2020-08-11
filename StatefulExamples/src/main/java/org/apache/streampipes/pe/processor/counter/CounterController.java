package org.apache.streampipes.pe.processor.counter;


import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.*;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.vocabulary.SO;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StandaloneEventProcessingDeclarer;

public class CounterController extends StandaloneEventProcessingDeclarer<CounterParameters> {

	private static final String EXAMPLE_KEY = "example-key";

	@Override
	public DataProcessorDescription declareModel() {
		return ProcessingElementBuilder.create("counter")
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
	public ConfiguredEventProcessor<CounterParameters> onInvocation
				(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

		String exampleString = extractor.singleValueParameter(EXAMPLE_KEY, String.class);

		CounterParameters params = new CounterParameters(graph, exampleString);

		return new ConfiguredEventProcessor<>(params, Counter::new);
	}

}
