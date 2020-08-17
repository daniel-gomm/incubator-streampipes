package org.apache.streampipes.state.serializers;

import com.google.gson.Gson;

import java.lang.reflect.Type;

public class GsonSerializer implements StateSerializer {

    Gson gson = new Gson();


    @Override
    public String serialize(Object o) {
        return gson.toJson(o);
    }

    @Override
    public <T> T deserialize(String serializedObject, Type type) {
        return gson.fromJson(serializedObject, type);
    }

}
