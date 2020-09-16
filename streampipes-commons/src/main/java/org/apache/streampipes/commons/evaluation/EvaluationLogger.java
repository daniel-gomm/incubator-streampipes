package org.apache.streampipes.commons.evaluation;

import com.sun.management.OperatingSystemMXBean;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class EvaluationLogger {

    private static HashMap<String, List<String[]>> tempLog = new HashMap();
    private static OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
            OperatingSystemMXBean.class);
    private static volatile boolean trackingResources = false;


    public static void addField(String name) {
        tempLog.put(name, new ArrayList<String[]>());
    }

    //Useless?
    public static void log(String field, Long... values) {
        String[] arr = new String[values.length];
        for(int i = 0; i<values.length; i++){
            arr[i] = Long.toString(values[i]);
        }
        log(field, arr);
    }

    public static void log(String field, Object... values) {
        String[] arr = new String[values.length];
        for(int i = 0; i<values.length; i++){
            arr[i] = values[i].toString();
        }
        log(field, arr);
    }

    public static void log(String field, String... values){
        if (!tempLog.containsKey(field)) {
            addField(field);
        }
        tempLog.get(field).add(values);
    }

    public static void logResources(Long interval) {
        trackingResources = true;
        Runnable runnable = () -> {
            while (trackingResources) {
                Long nextTime = System.currentTimeMillis() + interval;
                log("cpu", System.currentTimeMillis(), osBean.getSystemCpuLoad());
                log("memory", System.currentTimeMillis(), osBean.getFreePhysicalMemorySize());
                try {
                    Thread.sleep(Math.max(0, nextTime - System.currentTimeMillis()));
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
            synchronized (tempLog) {
                tempLog.notify();
            }
            trackingResources = false;
        };
        Thread t = new Thread(runnable);
        t.start();
    }

    public static void writeToFiles(){
        writeToFiles("results");
    }

    public static void writeToFiles(String folder) {
        if (trackingResources) {
            trackingResources = false;
            try {
                synchronized (tempLog) {
                    tempLog.wait();
                }
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
        for (Map.Entry<String, List<String[]>> entry : tempLog.entrySet()) {
            File f = new File(System.getProperty("user.home") + "/evaluation/results/" + folder +"/" + entry.getKey() + ".csv");
            if (!f.exists()) {
                try {
                    Files.createDirectories(f.getParentFile().toPath());
                    f.getAbsoluteFile().createNewFile();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            try (FileWriter fw = new FileWriter(f, true)) {
                for(int i = 0; i < entry.getValue().get(0).length; i++){
                    for (String[] ele : entry.getValue()) {
                        fw.append(ele[i] + ";");
                    }
                    fw.append("\n");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tempLog = new HashMap<>();
    }
}