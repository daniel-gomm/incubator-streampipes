package org.apache.streampipes.state.handling;

import org.apache.streampipes.state.serializers.StateSerializer;

public class ClassfulObject {
    String clazz;
    String object;

    public ClassfulObject(Object obj, StateSerializer serializer){
        this.clazz = obj.getClass().getName();
        this.object = serializer.serialize(obj);
    }
}
