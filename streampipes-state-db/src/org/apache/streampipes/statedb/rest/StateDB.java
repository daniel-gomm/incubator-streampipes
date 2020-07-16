package org.apache.streampipes.statedb.rest;

import com.google.gson.Gson;
import org.apache.streampipes.statedb.database.DataBaseInstances;
import org.apache.streampipes.statedb.database.PeStateDataBase;
import org.apache.streampipes.model.state.PipelineElementState;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/statedb")
public class StateDB {

    @POST
    @Path("/{elementId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String addState(@PathParam("elementId") String elementId, String payload){
        //TODO Doesn't return anything right now
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        if(db == null){
            db = DataBaseInstances.INSTANCE.add(elementId, new PeStateDataBase(elementId));
        }
        db.save(elementId + "-" + System.currentTimeMillis(), payload);
        return null;
    }


    @GET
    @Path("/{elementId}/last")
    @Produces(MediaType.APPLICATION_JSON)
    public String getLastState(@PathParam("elementId") String elementId){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        if(db == null){
            //Not found
            return null;
        }
        return db.find(db.getLastAdded());
    }

    @GET
    @Path("/{elementId}/{stateId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getState(@PathParam("elementId") String elementId, @PathParam("stateId") String stateId){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        if(db == null){
            //Not found
            return null;
        }
        return db.find(stateId);
    }


    @DELETE
    @Path("/{elementId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String removeDB(@PathParam("elementId") String elementId){
        if (DataBaseInstances.INSTANCE.remove(elementId)){
            return null;
        }else{
            return null;
        }
    }

    @POST
    @Path("/close")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String closeDB(){
        //TODO
        if (DataBaseInstances.INSTANCE.closeDataBases()){

        }else{

        }
        return null;
    }

    @POST
    @Path("/open")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String openDB(){
        //TODO load dabase from close state
        return null;
    }


    //Useless
    private String transformState(PipelineElementState state){
        Gson gson = new Gson();
        return gson.toJson(state);
    }

    private PipelineElementState transformToState(String state){
        return new Gson().fromJson(state, PipelineElementState.class);
    }
    //End of useless

}
