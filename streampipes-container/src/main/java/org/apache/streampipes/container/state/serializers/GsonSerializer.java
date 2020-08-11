package org.apache.streampipes.container.state.serializers;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;

import java.lang.reflect.Type;
import java.util.HashMap;

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
