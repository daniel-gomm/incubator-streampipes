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

package org.apache.streampipes.manager.execution.http;

import com.google.gson.Gson;
import com.google.gson.JsonSyntaxException;
import org.apache.http.client.ClientProtocolException;
import org.apache.http.client.fluent.Request;
import org.apache.http.client.fluent.Response;
import org.apache.http.entity.ContentType;
import org.apache.streampipes.model.state.PipelineElementState;
import org.apache.streampipes.model.state.StatefulPayload;
import org.eclipse.rdf4j.query.algebra.Str;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.streampipes.commons.Utils;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;
import org.apache.streampipes.model.client.pipeline.PipelineElementStatus;
import org.apache.streampipes.serializers.jsonld.JsonLdTransformer;

import java.io.IOException;

public class HttpRequestBuilder {

  private NamedStreamPipesEntity payload;
  private String belongsTo;

  private final static Logger LOG = LoggerFactory.getLogger(HttpRequestBuilder.class);

  public HttpRequestBuilder(NamedStreamPipesEntity payload, String belongsTo) {
    this.payload = payload;
    this.belongsTo = belongsTo;
  }

  //My code

  public String getState(){
    try {
      Response httpResp = Request.Get(belongsTo).connectTimeout(10000).execute();
      return httpResp.returnContent().asString();
    } catch (Exception e) {
      System.out.println(e.getMessage());
      //LOG.error("Could not get State " + belongsTo, e.getMessage());
      return "Failed in HttpRequestBuilder" + e.getMessage();
    }
  }

  public String setState(String state){
    try {
      Response httpResp =
              Request.Put(belongsTo).bodyString(state, ContentType.APPLICATION_JSON).connectTimeout(10000).execute();
      return httpResp.toString();
    } catch (Exception e) {
      //LOG.error(e.getMessage());
      System.out.println(e.getMessage());
      return "Failed in HttpRequestBuilder" + e.getMessage();
    }
  }

  //End of my code

  public PipelineElementStatus invoke() {
    LOG.info("Invoking element: " + belongsTo);
    try {
      String jsonLd = jsonLd();
      Response httpResp =
              Request.Post(belongsTo).bodyString(jsonLd, ContentType.APPLICATION_JSON).connectTimeout(10000).execute();
      return handleResponse(httpResp);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return new PipelineElementStatus(belongsTo, payload.getName(), false, e.getMessage());
    }
  }

  public PipelineElementStatus detach() {
    try {
      Response httpResp = Request.Delete(belongsTo).connectTimeout(10000).execute();
      System.out.println(this.belongsTo);
      return handleResponse(httpResp);
    } catch (Exception e) {
      LOG.error("Could not stop pipeline " + belongsTo, e.getMessage());
      return new PipelineElementStatus(belongsTo, payload.getName(), false, e.getMessage());
    }
  }

  private PipelineElementStatus handleResponse(Response httpResp) throws JsonSyntaxException, ClientProtocolException, IOException {
    String resp = httpResp.returnContent().asString();
    org.apache.streampipes.model.Response streamPipesResp = new Gson().fromJson(resp, org.apache.streampipes.model.Response.class);
    return convert(streamPipesResp);
  }


  private String jsonLd() throws Exception {
    return Utils.asString(new JsonLdTransformer().toJsonLd(payload));
  }

  private PipelineElementStatus convert(org.apache.streampipes.model.Response response) {
    return new PipelineElementStatus(belongsTo, payload.getName(), response.isSuccess(), response.getOptionalMessage());
  }

  //My code

  private String statefulJsonLd(PipelineElementState state) throws Exception{
    Gson gson = new Gson();
    StatefulPayload load = new StatefulPayload();
    load.pipelineElementState = state;
    load.namedStreamPipesEntity = jsonLd();
    return gson.toJson(load);
  }


  public PipelineElementStatus invoke(PipelineElementState state) {
    LOG.info("Invoking element: " + belongsTo);
    try {
      String statefulJsonLd = statefulJsonLd(state);
      Response httpResp =
              Request.Post(belongsTo).bodyString(statefulJsonLd, ContentType.APPLICATION_JSON).connectTimeout(10000).execute();
      return handleResponse(httpResp);
    } catch (Exception e) {
      LOG.error(e.getMessage());
      return new PipelineElementStatus(belongsTo, payload.getName(), false, e.getMessage());
    }
  }

  public PipelineElementStatus detachAndGetState(){
    try {
      Response httpResp = Request.Delete(belongsTo).connectTimeout(10000).execute();
      return handleResponse(httpResp);
    } catch (Exception e) {
      //Adjust
      LOG.error("Could not stop pipeline " + belongsTo, e.getMessage());
      return null;
    }
  }

  //End of my code

}
