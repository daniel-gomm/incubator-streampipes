package org.apache.streampipes.container.state.database;

import org.apache.streampipes.model.Response;

import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;

@Path("/statedb")
public class StateDB {

    @POST
    @Path("/{elementId}")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public static String addState(@PathParam("elementId") String elementId, String payload){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        try {
            if (db == null) {
                db = DataBaseInstances.INSTANCE.add(elementId, new PeStateDataBase(elementId));
            }
            String id = elementId + "-" + System.currentTimeMillis();
            db.save(id, payload);
            return new Response(elementId, true, id).toString();
        }catch(Exception e){
            return new Response(elementId, false, e.getMessage()).toString();
        }
    }


    @GET
    @Path("/{elementId}/last")
    @Produces(MediaType.APPLICATION_JSON)
    public String getLastState(@PathParam("elementId") String elementId){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        if(db == null){
            return new Response(elementId, false, "Database not found.").toString();
        }
        try {
            return new Response(elementId, true, db.getLastAdded()).toString();
        } catch(Exception e){
            return new Response(elementId, false, e.getMessage()).toString();
        }
    }

    @GET
    @Path("/{elementId}/{stateId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String getState(@PathParam("elementId") String elementId, @PathParam("stateId") String stateId){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        if(db == null){
            //Not found
            return new Response(elementId, false, "Database not found.").toString();
        }
        try {
            return new Response(elementId, true, db.find(stateId)).toString();
        } catch(Exception e){
            return new Response(elementId, false, e.getMessage()).toString();
        }
    }


    @DELETE
    @Path("/{elementId}")
    @Produces(MediaType.APPLICATION_JSON)
    public String removeDB(@PathParam("elementId") String elementId){
        if (DataBaseInstances.INSTANCE.remove(elementId)){
            return new Response(elementId, true).toString();
        }else{
            return new Response(elementId, false).toString();
        }
    }

    @GET
    @Path("/{elementId}/close")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public static String closeDB(@PathParam("elementId") String elementId){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        if(db == null){
            //Not found
            return new Response(elementId, false, "Database not found.").toString();
        }
        try {
            db.close();
            return new Response(elementId, true, "Database successfully closed.").toString();
        } catch(Exception e){
            return new Response(elementId, false, e.getMessage()).toString();
        }
    }


    @GET
    @Path("/{elementId}/open")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public static String openDB(@PathParam("elementId") String elementId){
        try{
            DataBaseInstances.INSTANCE.open(elementId);
            return new Response("open statedb", true).toString();
        }catch (Exception e){
            return new Response("open statedb", false, e.getMessage()).toString();
        }
    }



    @GET
    @Path("/close")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String closeAllDBs(){
        if (DataBaseInstances.INSTANCE.closeDataBases()){
            return new Response("statedb", true).toString();
        }else{
            return new Response("statedb", false).toString();
        }
    }

    @GET
    @Path("/open")
    @Consumes(MediaType.APPLICATION_JSON)
    @Produces(MediaType.APPLICATION_JSON)
    public String openAllDBs(){
        try{
            DataBaseInstances.INSTANCE.openDataBases();
            return new Response("open statedb", true).toString();
        }catch (Exception e){
            return new Response("open statedb", false, e.getMessage()).toString();
        }
    }

    //Just for debugging purposes
    @GET
    @Path("/{elementId}/seek")
    @Produces(MediaType.APPLICATION_JSON)
    public String seek(@PathParam("elementId") String elementId){
        PeStateDataBase db = DataBaseInstances.INSTANCE.getDataBase(elementId);
        db.seek();
        return new Response("seek statedb", true).toString();
    }

}
