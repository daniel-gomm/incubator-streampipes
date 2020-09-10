/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.streampipes.state.handling;

import com.google.common.reflect.TypeToken;
import org.apache.streampipes.state.annotations.StateObject;
import org.apache.streampipes.state.serializers.GsonSerializer;
import org.apache.streampipes.state.serializers.StateSerializer;

import java.lang.reflect.Field;
import java.lang.reflect.Type;
import java.util.*;

public class StateHandler {
    private ArrayList<Field> fields;
    private HashMap<String, Field> fieldsMap;
    private Object obj;
    private StateSerializer serializer;


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
                ClassfulObject o = new ClassfulObject(f.get(this.obj), this.serializer);
                list.put(f.getName(), o);
            } catch (IllegalAccessException e) {
                e.printStackTrace();
            }
        }
        return serializer.serialize(list);
    }
}
