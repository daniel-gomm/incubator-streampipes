package org.apache.streampipes.state.serializers;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.lang.reflect.Type;

public class JacksonSerializer implements StateSerializer {

    ObjectMapper objectMapper = new ObjectMapper();

    @Override
    public String serialize(Object o) {
        try {
            return objectMapper.writeValueAsString(o);
        } catch (JsonProcessingException e) {
            e.printStackTrace();
        }
        return null;
    }

    @Override
    public <T> T deserialize(String serializedObject, Type type) {
        if (type instanceof Class){
            try {
                return objectMapper.readValue(serializedObject, (Class<T>)type);
            } catch (JsonProcessingException e) {
                e.printStackTrace();
            }
        }
        return null;
    }
}
