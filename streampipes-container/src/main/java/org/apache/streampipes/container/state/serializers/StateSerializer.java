package org.apache.streampipes.container.state.serializers;

import java.lang.reflect.Type;
import java.util.HashMap;

public interface StateSerializer {
    String serialize(Object o);
    <T> T deserialize(String serializedObject, Type type);
}
