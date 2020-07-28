package org.apache.streampipes.manager.execution.http;

import com.google.gson.Gson;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.streampipes.model.monitoring.StreamPipesRuntimeError;
import org.apache.streampipes.model.state.PipelineElementState;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

public class StateDBConnector {

    public static String url = "http://localhost:8082/streampipes-backend/api/statedb/";

    public static boolean saveState(Map<String, PipelineElementState> states){
        boolean failed = false;
        for (Map.Entry<String, PipelineElementState> entry : states.entrySet()) {
            try {
                Response resp = Request.Post(url + entry.getKey().split("/")[entry.getKey().split("/").length - 1])
                        .bodyString(new Gson().toJson(entry.getValue()), ContentType.APPLICATION_JSON).connectTimeout(10000).execute();
                String respString = resp.returnContent().asString();
                org.apache.streampipes.model.Response streamPipesResp = new Gson().fromJson(respString, org.apache.streampipes.model.Response.class);
                if(!streamPipesResp.isSuccess()){
                    failed = true;
                }
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        if (failed){
            return false;
        }else{
            return true;
        }
    }

    public static PipelineElementState findState(String element, String stateID){
        try {
            Response resp = Request.Get(url + element + "/" + stateID).connectTimeout(10000).execute();
            String respString = resp.returnContent().asString();
            org.apache.streampipes.model.Response streamPipesResp = new Gson().fromJson(respString, org.apache.streampipes.model.Response.class);
            return new Gson().fromJson(streamPipesResp.getOptionalMessage(), PipelineElementState.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

    public static PipelineElementState findLastState(String element){
        try {
            Response resp = Request.Get(url + element + "/last").connectTimeout(10000).execute();
            String respString = resp.returnContent().asString();
            org.apache.streampipes.model.Response streamPipesResp = new Gson().fromJson(respString, org.apache.streampipes.model.Response.class);
            return new Gson().fromJson(streamPipesResp.getOptionalMessage(), PipelineElementState.class);
        } catch (IOException e) {
            e.printStackTrace();
        }
        return null;
    }

}
