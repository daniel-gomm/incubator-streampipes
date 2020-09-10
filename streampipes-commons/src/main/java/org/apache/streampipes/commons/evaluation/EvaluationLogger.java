package org.apache.streampipes.commons.evaluation;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluationLogger {

    private static HashMap<String, List<String>> loggedData = new HashMap<>();

    public static void close(){
        File f = new File("/home/daniel/results/evaluation_log_" + System.currentTimeMillis() + ".txt");
        if (!f.exists()){
            try {
                Files.createDirectories(f.getParentFile().toPath());
                f.getAbsoluteFile().createNewFile();
            } catch(IOException ex){
                ex.printStackTrace();
            }
        }
        try(FileWriter fw = new FileWriter(f)){
            fw.append("Evaluation results\n");
            for(Map.Entry<String, List<String>> entry : loggedData.entrySet()){
                fw.append(entry.getKey() + "\n");
                for(String ele : entry.getValue()){
                    fw.append(ele + ";");
                }
                fw.append("\n");
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        loggedData = new HashMap<>();
    }

    public static void addField(String name){
        loggedData.put(name, new ArrayList<String>());
    }

    public static void log(String field, String value){
        loggedData.get(field).add(value);
    }

}
