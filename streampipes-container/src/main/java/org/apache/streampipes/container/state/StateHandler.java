package org.apache.streampipes.container.state;

import com.google.gson.Gson;
import org.apache.streampipes.container.state.annotations.StateObject;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

public class StateHandler {

    private ArrayList<Field> fields;
    private HashMap<String, Field> fieldsMap;
    private Object obj;
    private String lastState;

    public StateHandler(Object o){
        this.obj = o;
        this.fields = new ArrayList<Field>(Arrays.asList(o.getClass().getFields()));

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
        Gson gson = new Gson();
        HashMap<String, Object> stateMap = gson.fromJson(state, HashMap.class);

        for(Map.Entry<String, Object> entry : stateMap.entrySet()){
            try {
                this.fieldsMap.get(entry.getKey()).set(this.obj, this.fieldsMap.get(entry.getKey()).get(this.obj).getClass().cast(entry.getValue()));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
    }


    public String getState()  {
        Map<String, Object> list = new HashMap<>();
        for(Field f : this.fields){
            try {
                list.put(f.getName(),f.get(this.obj));
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        Gson gson = new Gson();
        this.lastState = gson.toJson(list);
        return this.lastState;
    }

    public String difference(){
        HashMap<String, Object> oldMap = new Gson().fromJson(this.lastState, HashMap.class);
        HashMap<String, Object> changes = new HashMap<>();
        for(Map.Entry<String, Object> entry : oldMap.entrySet()){
            try {
                if (!entry.getValue().equals(this.fieldsMap.get(entry.getKey()).get(this.obj))) {
                    changes.put(entry.getKey(), this.fieldsMap.get(entry.getKey()).get(this.obj));
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return new Gson().toJson(changes);
    }

}
