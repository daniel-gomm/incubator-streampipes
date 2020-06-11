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

package org.apache.streampipes.pe.processor.counter;

import org.apache.streampipes.model.DataProcessorType;
import org.apache.streampipes.model.Response;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataProcessorInvocation;
import org.apache.streampipes.sdk.builder.ProcessingElementBuilder;
import org.apache.streampipes.sdk.builder.StreamRequirementsBuilder;
import org.apache.streampipes.sdk.extractor.ProcessingElementParameterExtractor;
import org.apache.streampipes.sdk.helpers.EpRequirements;
import org.apache.streampipes.sdk.helpers.Labels;
import org.apache.streampipes.sdk.helpers.Locales;
import org.apache.streampipes.sdk.helpers.OutputStrategies;
import org.apache.streampipes.sdk.utils.Assets;
import org.apache.streampipes.wrapper.standalone.ConfiguredEventProcessor;
import org.apache.streampipes.wrapper.standalone.declarer.StatefulStandaloneEventProcessingDeclarer;

public class CounterController extends StatefulStandaloneEventProcessingDeclarer<CounterParameters> {

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
						.outputStrategy(OutputStrategies.keep())
						.build();
	}

	@Override
	public ConfiguredEventProcessor<CounterParameters> onInvocation
				(DataProcessorInvocation graph, ProcessingElementParameterExtractor extractor) {

		String exampleString = extractor.singleValueParameter(EXAMPLE_KEY, String.class);

		CounterParameters params = new CounterParameters(graph, exampleString);

		return new ConfiguredEventProcessor<>(params, Counter::new);
	}

	@Override
	public Response getState() {
		return null;
	}

	@Override
	public Response setState(String state) {
		return null;
	}
}
