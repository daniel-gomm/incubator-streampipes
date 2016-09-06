package de.fzi.cep.sepa.manager.matching.v2;

import java.util.List;

import de.fzi.cep.sepa.model.client.matching.MatchingResultMessage;
import de.fzi.cep.sepa.model.client.matching.MatchingResultType;
import de.fzi.cep.sepa.model.impl.eventproperty.EventProperty;
import de.fzi.cep.sepa.model.impl.eventproperty.EventPropertyList;
import de.fzi.cep.sepa.model.impl.eventproperty.EventPropertyNested;
import de.fzi.cep.sepa.model.impl.eventproperty.EventPropertyPrimitive;

public class PropertyMatch extends AbstractMatcher<EventProperty, EventProperty> {

	public PropertyMatch() {
		super(MatchingResultType.PROPERTY_MATCH);
		// TODO Auto-generated constructor stub
	}

	@Override
	public boolean match(EventProperty offer, EventProperty requirement, List<MatchingResultMessage> errorLog) {
		if (!matchesType(offer, requirement)) return false; 
		else {
			if (isPrimitive(requirement)) return new PrimitivePropertyMatch().match(toPrimitive(offer), toPrimitive(requirement), errorLog);
			else if (isList(requirement)) return new ListPropertyMatch().match(toList(offer), toList(requirement), errorLog);
			else if (isNested(requirement)) return new NestedPropertyMatch().match(toNested(offer), toNested(requirement), errorLog);
			else return false;
		}
	}
	
	private EventPropertyNested toNested(EventProperty property) {
		return (EventPropertyNested) property;
	}

	private EventPropertyList toList(EventProperty property) {
		return (EventPropertyList) property;
	}

	private EventPropertyPrimitive toPrimitive(EventProperty property) {
		return (EventPropertyPrimitive) property;
	}

	private boolean isList(EventProperty requirement) {
		return requirement instanceof EventPropertyPrimitive;
	}
	
	private boolean isNested(EventProperty requirement) {
		return requirement instanceof EventPropertyNested;
	}
	
	private boolean isPrimitive(EventProperty requirement) {
		return requirement instanceof EventPropertyPrimitive;
	}

	public boolean matchesType(EventProperty offer, EventProperty requirement) {
		return offer.getClass().getCanonicalName().equals(requirement.getClass().getCanonicalName());
	}
	
	
	

}
