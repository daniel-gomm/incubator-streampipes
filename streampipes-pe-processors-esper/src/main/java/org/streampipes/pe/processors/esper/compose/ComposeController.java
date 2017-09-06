package org.streampipes.pe.processors.esper.compose;

import com.clarkparsia.empire.annotation.InvalidRdfException;
import org.openrdf.rio.RDFHandlerException;
import org.streampipes.commons.Utils;
import org.streampipes.container.util.StandardTransportFormat;
import org.streampipes.model.impl.EventStream;
import org.streampipes.model.impl.graph.SepaDescription;
import org.streampipes.model.impl.graph.SepaInvocation;
import org.streampipes.model.impl.output.OutputStrategy;
import org.streampipes.model.impl.output.RenameOutputStrategy;
import org.streampipes.model.impl.staticproperty.StaticProperty;
import org.streampipes.model.transform.JsonLdTransformer;
import org.streampipes.pe.processors.esper.config.EsperConfig;
import org.streampipes.wrapper.ConfiguredEventProcessor;
import org.streampipes.wrapper.runtime.EventProcessor;
import org.streampipes.wrapper.standalone.declarer.StandaloneEventProcessorDeclarerSingleton;

import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.List;

public class ComposeController extends StandaloneEventProcessorDeclarerSingleton<ComposeParameters> {

	@Override
	public SepaDescription declareModel() {
		
		EventStream stream1 = new EventStream();
		EventStream stream2 = new EventStream();
		
		SepaDescription desc = new SepaDescription("compose", "Compose EPA", "");
		
		stream1.setUri(EsperConfig.serverUrl +"/" +Utils.getRandomString());
		stream2.setUri(EsperConfig.serverUrl +"/" +Utils.getRandomString());
		desc.addEventStream(stream1);
		desc.addEventStream(stream2);
		
		List<OutputStrategy> strategies = new ArrayList<OutputStrategy>();
		strategies.add(new RenameOutputStrategy());
		desc.setOutputStrategies(strategies);
		
		List<StaticProperty> staticProperties = new ArrayList<StaticProperty>();
		
		//staticProperties.add(new MatchingStaticProperty("select matching", ""));
		desc.setStaticProperties(staticProperties);
		desc.setSupportedGrounding(StandardTransportFormat.getSupportedGrounding());
		return desc;
	}

	@Override
	public ConfiguredEventProcessor<ComposeParameters, EventProcessor<ComposeParameters>> onInvocation(SepaInvocation
																																																							 sepa) {
		try {
			System.out.println(Utils.asString(new JsonLdTransformer().toJsonLd(sepa)));
		} catch (RDFHandlerException | IllegalAccessException
						| IllegalArgumentException | InvocationTargetException
						| SecurityException | ClassNotFoundException
						| InvalidRdfException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		ComposeParameters staticParam = new ComposeParameters(sepa);

		return new ConfiguredEventProcessor<>(staticParam, Compose::new);
	}

}
