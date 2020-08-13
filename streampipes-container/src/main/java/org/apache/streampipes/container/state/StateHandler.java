package org.apache.streampipes.container.state;

import com.google.common.reflect.TypeToken;
import org.apache.streampipes.container.state.annotations.StateObject;
import org.apache.streampipes.container.state.serializers.GsonSerializer;
import org.apache.streampipes.container.state.serializers.StateSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

public class StateHandler {
    private ArrayList<Field> fields;
    private HashMap<String, Field> fieldsMap;
    private Object obj;
    private String currentState;
    private StateSerializer serializer;

    private class ClassfulObject{
        String clazz;
        String object;

        public ClassfulObject(Object obj){
            this.clazz = obj.getClass().getName();
            this.object = serializer.serialize(obj);
        }
    }

    public StateHandler(Object o){
        this(o, new GsonSerializer());
    }

    public StateHandler(Object o, StateSerializer serializer){
        this.obj = o;
        this.fields = new ArrayList<Field>(Arrays.asList(o.getClass().getFields()));
        this.serializer = serializer;
        //Only keep marked fields as part of the State
        for(Field f : o.getClass().getFields()){
            if(f.getAnnotation(StateObject.class) == null){
                this.fields.remove(f);
            }
        }

        this.fieldsMap = new HashMap<String, Field>();
        //Make a map of all fields with their respective Names
        for(Field f: fields){
            this.fieldsMap.put(f.getName(), f);
        }
    }

    public void setState(String state){
        Type t = new TypeToken<HashMap<String, ClassfulObject>>(){}.getType();
        HashMap<String, ClassfulObject> map = serializer.deserialize(state, t);
        for(Map.Entry<String, ClassfulObject> entry : map.entrySet()){
            try {
                this.fieldsMap.get(entry.getKey()).set(this.obj, serializer.deserialize(entry.getValue().object, Class.forName(entry.getValue().clazz)));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            } catch ( ClassNotFoundException nfe){
                nfe.printStackTrace();
            }
        }

    }

    public String getState()  {
        Map<String, ClassfulObject> list = new HashMap<>();
        for(Field f : this.fields){
            try {
                ClassfulObject o = new ClassfulObject(f.get(this.obj));
                list.put(f.getName(), o);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        this.currentState = serializer.serialize(list);
        return this.currentState;
    }
}
