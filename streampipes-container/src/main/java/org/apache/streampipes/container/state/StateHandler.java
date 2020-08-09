package org.apache.streampipes.container.state;

import com.google.common.reflect.TypeToken;
import com.google.gson.Gson;
import org.apache.streampipes.container.declarer.StatefulInvocableDeclarer;
import org.apache.streampipes.container.state.annotations.StateObject;
import org.apache.streampipes.container.state.database.StateDB;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

public class StateHandler{

    private ArrayList<Field> fields;
    private HashMap<String, Field> fieldsMap;
    private Object obj;
    private String currentState;
    private final String elementId;

    private class ClassfulObject{
        String clazz;
        String object;

        public ClassfulObject(Object obj){
            this.clazz = obj.getClass().getName();
            this.object = new Gson().toJson(obj);
        }
    }

    public StateHandler(Object o, String elementId){
        this.obj = o;
        this.fields = new ArrayList<Field>(Arrays.asList(o.getClass().getFields()));
        this.elementId = elementId.split("/")[2].replace(":", "") + elementId.split("/")[elementId.split("/").length - 1];
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
        StateDB.openDB(elementId);
    }

    public void setState(String state){
        Gson gson = new Gson();
        Type t = new TypeToken<HashMap<String, ClassfulObject>>(){}.getType();
        HashMap<String, ClassfulObject> map = new Gson().fromJson(state, t);
        for(Map.Entry<String, ClassfulObject> entry : map.entrySet()){
            try {
                this.fieldsMap.get(entry.getKey()).set(this.obj, gson.fromJson(entry.getValue().object, Class.forName(entry.getValue().clazz)));
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
        Gson gson = new Gson();
        this.currentState = gson.toJson(list);
        return this.currentState;
    }

    public String difference(){
        Type t = new TypeToken<HashMap<String, ClassfulObject>>(){}.getType();
        HashMap<String, ClassfulObject> oldMap = new Gson().fromJson(this.currentState, t);
        HashMap<String, ClassfulObject> changes = new HashMap<>();
        for(Map.Entry<String, ClassfulObject> entry : oldMap.entrySet()){
            try {
                if (!new Gson().fromJson(entry.getValue().object, Class.forName(entry.getValue().clazz)).equals(this.fieldsMap.get(entry.getKey()).get(this.obj))) {
                    changes.put(entry.getKey(), new ClassfulObject(this.fieldsMap.get(entry.getKey()).get(this.obj)));
                }
            } catch (Exception e){
                e.printStackTrace();
            }
        }
        return new Gson().toJson(changes);
    }

    public void saveState(){
        StateDB.addState(this.elementId, getState());
    }

    public void closeDB(){
        StateDB.closeDB(elementId);
    }

    /**Useless
     private HashMap<String, ClassfulObject> modifyMap(HashMap<String, String> map){
     HashMap<String, ClassfulObject> mapster = new HashMap<String, ClassfulObject>();
     Gson gson = new Gson();
     for(Map.Entry<String, String> e : map.entrySet()){
     mapster.put(e.getKey(), gson.fromJson(e.getValue(), ClassfulObject.class));
     }
     return mapster;
     }



    private Map<String, Object> findDifference(Object oldObject, Object newObject){
        HashMap<String, Object> changes = new HashMap();
        if(oldObject.getClass() != newObject.getClass()){
            //Objects are of different class
        }

        for(Field f : oldObject.getClass().getDeclaredFields()){
            try {
                if(!f.get(oldObject).equals(f.get(newObject))){
                    if(f.get(oldObject).getClass() == Byte.class || f.get(oldObject).getClass() == Short.class || f.get(oldObject).getClass() == Integer.class
                            || f.get(oldObject).getClass() == Long.class || f.get(oldObject).getClass() == Float.class || f.get(oldObject).getClass() == Double.class
                            || f.get(oldObject).getClass() == Boolean.class || f.get(oldObject).getClass() == Character.class){
                        //Is primitive data type
                    }
                    else if(f.get(oldObject).getClass().isAssignableFrom(Collection.class)){
                        //Is collection
                    } else{
                        //Is something else
                    }
                }
                //Change right here
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }

        return changes;
    }**/

}