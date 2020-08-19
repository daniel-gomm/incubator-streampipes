package org.apache.streampipes.wrapper.routing;

import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.messaging.InternalEventProcessor;
import org.apache.streampipes.model.runtime.Event;

import java.util.Map;

public class DiscardingOutputCollector implements SpOutputCollector {
    @Override
    public void collect(Event event) {

    }

    @Override
    public void registerConsumer(String routeId, InternalEventProcessor<Map<String, Object>> consumer) {

    }

    @Override
    public void unregisterConsumer(String routeId) {

    }

    @Override
    public void connect() throws SpRuntimeException {

    }

    @Override
    public void disconnect() throws SpRuntimeException {

    }
}
