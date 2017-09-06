package org.streampipes.pe.processors.esper.pattern.sequence;

import java.util.List;

import org.streampipes.model.impl.graph.SepaInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

public class SequenceParameters extends EventProcessorBindingParams {

	private String timeUnit;
	private String matchingOperator;
	private int duration;
	private List<String> matchingProperties;
	
	
	public SequenceParameters(SepaInvocation invocationGraph, String timeUnit,
			String matchingOperator, int duration, List<String> matchingProperties) {
		super(invocationGraph);
		this.timeUnit = timeUnit;
		this.matchingOperator = matchingOperator;
		this.duration = duration;
		this.matchingProperties = matchingProperties;
	}


	public String getTimeUnit() {
		return timeUnit;
	}


	public String getMatchingOperator() {
		return matchingOperator;
	}


	public int getDuration() {
		return duration;
	}


	public List<String> getMatchingProperties() {
		return matchingProperties;
	}
	
	

}
