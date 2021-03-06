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

package org.apache.streampipes.manager.execution.status;

import java.util.HashMap;
import java.util.Map;

import org.apache.streampipes.manager.monitoring.runtime.PipelineObserver;
import org.apache.streampipes.manager.monitoring.runtime.SepStoppedMonitoring;

public class SepMonitoringManager {

	public static SepStoppedMonitoring SepMonitoring;
	
	private static Map<String, PipelineObserver> observers;
	
	static {
		SepMonitoring = new SepStoppedMonitoring();
		observers = new HashMap<>();
		Thread thread = new Thread(SepMonitoring);
		thread.start();
	}
	
	public static void addObserver(String pipelineId) {
		PipelineObserver observer = new PipelineObserver(pipelineId);
		observers.put(pipelineId, observer);
		SepMonitoring.register(observer);
	}
	
	public static void removeObserver(String pipelineId) {
		SepMonitoring.remove(observers.get(pipelineId));
	}
	
}
