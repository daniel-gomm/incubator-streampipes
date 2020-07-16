package org.apache.streampipes.statedb.database;

import java.util.HashMap;
import java.util.Map;

public enum DataBaseInstances {
    INSTANCE;

    private final Map<String, PeStateDataBase> openDataBases = new HashMap<>();

    public PeStateDataBase add(String key, PeStateDataBase dataBase){
        return openDataBases.put(key, dataBase);
    }

    public boolean remove(String key){
        PeStateDataBase db = openDataBases.remove(key);
        if (db == null){
            return false;
        }else{
            return true;
        }
    }

    public PeStateDataBase getDataBase(String key){
        return openDataBases.get(key);
    }

    public boolean closeDataBases(){
        try {
            for (Map.Entry<String, PeStateDataBase> entry : openDataBases.entrySet()) {
                entry.getValue().close();
            }
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

}
