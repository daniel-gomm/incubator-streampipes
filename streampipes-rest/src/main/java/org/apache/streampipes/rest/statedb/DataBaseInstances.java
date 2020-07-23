
package org.apache.streampipes.rest.statedb;

import java.io.File;
import java.util.HashMap;
import java.util.Map;

public enum DataBaseInstances {
    INSTANCE;

    private final Map<String, PeStateDataBase> openDataBases = new HashMap<>();

    public PeStateDataBase add(String key, PeStateDataBase dataBase){
        openDataBases.put(key, dataBase);
        return openDataBases.get(key);
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
                openDataBases.remove(entry);
            }
        }catch(Exception e){
            e.printStackTrace();
            return false;
        }
        return true;
    }

    public void openDataBases(){
        File[] dirs = new File("/tmp/streampipes/rocks-db").listFiles();
        for (File dir : dirs){
            if (dir.isDirectory()){
                openDataBases.put(dir.getPath().replace("/tmp/streampipes/rocks-db/", ""),
                        new PeStateDataBase(dir.getPath().replace("/tmp/streampipes/rocks-db/", "")));
            }
        }
    }

}


