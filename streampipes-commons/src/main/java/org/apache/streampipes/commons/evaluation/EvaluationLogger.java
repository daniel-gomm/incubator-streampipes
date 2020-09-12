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

    private static HashMap<String, List<String>> tempLog = new HashMap();
    private static OperatingSystemMXBean osBean = ManagementFactory.getPlatformMXBean(
            OperatingSystemMXBean.class);
    private static volatile boolean trackingResources = false;


    public static void addField(String name) {
        tempLog.put(name, new ArrayList<String>());
    }

    public static void log(String field, String value) {
        if (!tempLog.containsKey(field)) {
            addField(field);
        }
        tempLog.get(field).add(value);
    }

    public static void log(String field, Long value) {
        log(field, Long.toString(value));
    }

    public static void logResources(Long interval) {
        trackingResources = true;
        Runnable runnable = () -> {
            while (trackingResources) {
                Long nextTime = System.currentTimeMillis() + interval;
                log("cpu", osBean.getSystemCpuLoad() + ";" + System.currentTimeMillis());
                log("memory", osBean.getFreePhysicalMemorySize() + ";" + System.currentTimeMillis());
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
        for (Map.Entry<String, List<String>> entry : tempLog.entrySet()) {
            File f = new File("/home/daniel/evaluation/" + folder +"/" + entry.getKey() + ".txt");
            if (!f.exists()) {
                try {
                    Files.createDirectories(f.getParentFile().toPath());
                    f.getAbsoluteFile().createNewFile();
                } catch (IOException ex) {
                    ex.printStackTrace();
                }
            }
            try (FileWriter fw = new FileWriter(f)) {
                for (String ele : entry.getValue()) {
                    fw.append(ele + ";");
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        tempLog = new HashMap<>();
    }
}