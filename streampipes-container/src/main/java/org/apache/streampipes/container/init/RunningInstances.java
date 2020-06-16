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

package org.apache.streampipes.container.init;

import org.apache.streampipes.container.declarer.InvocableDeclarer;
import org.apache.streampipes.container.declarer.StatefulInvocableDeclarer;
import org.apache.streampipes.container.util.ElementInfo;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;

import java.util.HashMap;
import java.util.Map;

public enum RunningInstances {
    INSTANCE;

    private final Map<String, ElementInfo<NamedStreamPipesEntity, InvocableDeclarer>> runningInstances = new HashMap<>();
    private final Map<String, ElementInfo<NamedStreamPipesEntity, StatefulInvocableDeclarer>> runningStatefulInstances = new HashMap<>(); //Added


    public void add(String id, NamedStreamPipesEntity description, InvocableDeclarer invocation) {
        runningInstances.put(id, new ElementInfo<>(description, invocation));
        //My code
        if (invocation instanceof StatefulInvocableDeclarer){
            addStateful(id, description, (StatefulInvocableDeclarer) invocation);
            System.out.println("Added stateful");
        }//end of my code

    }

    public InvocableDeclarer getInvocation(String id) {
        ElementInfo<NamedStreamPipesEntity, InvocableDeclarer> result = runningInstances.get(id);
        //My code
        if (result.getInvocation() instanceof StatefulInvocableDeclarer){
            return (StatefulInvocableDeclarer) result.getInvocation();
        } //end of my code
        else if (result != null) {
            return result.getInvocation();
        } else {
            return null;
        }
    }

    //My code

    public void addStateful(String id, NamedStreamPipesEntity description, StatefulInvocableDeclarer invocation){
        runningStatefulInstances.put(id, new ElementInfo<>(description, invocation));
    }

    public StatefulInvocableDeclarer getStatefulInvocation(String id){
        ElementInfo<NamedStreamPipesEntity, StatefulInvocableDeclarer> result = runningStatefulInstances.get(id);
        if (result != null) {
            return result.getInvocation();
        } else {
            return null;
        }
    }

    //End of my code

    public NamedStreamPipesEntity getDescription(String id) {
        return runningInstances.get(id).getDescription();
    }

    public void remove(String id) {
        runningInstances.remove(id);
        runningStatefulInstances.remove(id); //Added
    }

    public Integer getRunningInstancesCount() {
        return runningInstances.size();
    }
}
