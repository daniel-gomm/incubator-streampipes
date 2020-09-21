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

package org.apache.streampipes.examples.pe.processor.aggregator;

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

public class AggregatorController extends StandaloneEventProcessingDeclarer<AggregatorParameters> {

	private static final String WINDOW_SIZE_KEY = "window-size";
	private static final String STATE_SIZE_KEY = "state-size";

	@Override
	public DataProcessorDescription declareModel() {
		return ProcessingElementBuilder.create("org.apache.streampipes.examples.pe.processor.aggregator")
				.withAssets(Assets.DOCUMENTATION)
				.withLocales(Locales.EN)
				.category(DataProcessorType.AGGREGATE)
				.requiredStream(StreamRequirementsBuilder
						.create()
						.requiredProperty(EpRequirements.anyProperty())
						.build())
				.requiredIntegerParameter(Labels.withId(WINDOW_SIZE_KEY))
				.requiredIntegerParameter(Labels.withId(STATE_SIZE_KEY))
				.supportedFormats(SupportedFormats.jsonFormat())
				.supportedProtocols(SupportedProtocols.kafka())
				.outputStrategy(OutputStrategies.append(EpProperties.integerEp(Labels.from("aggregated","Aggregation over events","Reflects what the aggregated value."), "cnt", SO.Number)))
				.build();
	}

	@Override
	public ConfiguredEventProcessor<AggregatorParameters> onInvocation
				(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

		int window_size = extractor.singleValueParameter(WINDOW_SIZE_KEY, Integer.class);
		int state_size = extractor.singleValueParameter(STATE_SIZE_KEY, Integer.class);

		AggregatorParameters params = new AggregatorParameters(graph, window_size, state_size);

		return new ConfiguredEventProcessor<>(params, Aggregator::new);
	}

}
