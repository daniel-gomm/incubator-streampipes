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
package org.apache.streampipes.backend;

import org.apache.shiro.web.env.EnvironmentLoaderListener;
import org.apache.shiro.web.servlet.OncePerRequestFilter;
import org.apache.shiro.web.servlet.ShiroFilter;
import org.apache.streampipes.state.rocksdb.StateDatabase;
import org.glassfish.jersey.servlet.ServletContainer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.EnableAutoConfiguration;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletListenerRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.apache.streampipes.app.file.export.application.AppFileExportApplication;
import org.apache.streampipes.manager.operations.Operations;
import org.apache.streampipes.model.client.pipeline.PipelineOperationStatus;
import org.apache.streampipes.rest.notifications.NotificationListener;

import java.util.List;

import javax.annotation.PreDestroy;
import javax.servlet.ServletContextListener;

@Configuration
@EnableAutoConfiguration
@Import({ StreamPipesResourceConfig.class, WelcomePageController.class })
public class StreamPipesBackendApplication {

  private static final Logger LOG = LoggerFactory.getLogger(StreamPipesBackendApplication.class.getCanonicalName());

  public static void main(String[] args) {
    System.setProperty("org.apache.tomcat.util.buf.UDecoder.ALLOW_ENCODED_SLASH", "true");
    StateDatabase.DATABASE.initialize("/tmp/streampipes/rocksdb/backend");
    SpringApplication.run(StreamPipesBackendApplication.class, args);
  }

  @PreDestroy
  public void onExit() {
   LOG.info("Shutting down StreamPipes...");
    List<PipelineOperationStatus> status = Operations.stopAllPipelines();
    status.forEach(s -> {
      if (s.isSuccess()) {
        LOG.info("Pipeline {} successfully stopped", s.getPipelineName());
      } else {
        LOG.error("Pipeline {} could not be stopped", s.getPipelineName());
      }
    });
    StateDatabase.DATABASE.close();
  }

  @Bean
  public ServletRegistrationBean appFileExportRegistrationBean() {
    ServletContainer jerseyContainer = new ServletContainer(new AppFileExportApplication());
    return new ServletRegistrationBean<>(jerseyContainer, "/api/apps/*");
  }

  @Bean
  public FilterRegistrationBean shiroFilterBean() {
    FilterRegistrationBean<OncePerRequestFilter> bean = new FilterRegistrationBean<>();
    bean.setFilter(new ShiroFilter());
    bean.addUrlPatterns("/api/*");
    return bean;
  }

  @Bean
  public ServletListenerRegistrationBean shiroListenerBean() {
    return listener(new EnvironmentLoaderListener());
  }

  @Bean
  public ServletListenerRegistrationBean streamPipesNotificationListenerBean() {
    return listener(new NotificationListener());
  }

  private ServletListenerRegistrationBean listener(ServletContextListener listener) {
    ServletListenerRegistrationBean<ServletContextListener> bean =
            new ServletListenerRegistrationBean<>();
    bean.setListener(listener);
    return bean;
  }

}
