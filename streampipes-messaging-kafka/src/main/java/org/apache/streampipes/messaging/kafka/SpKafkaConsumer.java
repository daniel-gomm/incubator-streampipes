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

package org.apache.streampipes.messaging.kafka;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.TopicPartition;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.streampipes.commons.exceptions.SpRuntimeException;
import org.apache.streampipes.messaging.EventConsumer;
import org.apache.streampipes.messaging.InternalEventProcessor;
import org.apache.streampipes.messaging.kafka.config.ConsumerConfigFactory;
import org.apache.streampipes.model.grounding.KafkaTransportProtocol;
import org.apache.streampipes.model.grounding.SimpleTopicDefinition;
import org.apache.streampipes.model.grounding.WildcardTopicDefinition;

import java.io.Serializable;
import java.util.*;
import java.util.regex.Pattern;

public class SpKafkaConsumer implements EventConsumer<KafkaTransportProtocol>, Runnable,
        Serializable {

  private String topic;
  private InternalEventProcessor<byte[]> eventProcessor;
  private KafkaTransportProtocol protocol;
  private volatile boolean isRunning;
  private volatile boolean isFinished = false;
  private Boolean patternTopic = false;

  //My code

  private Long offset;
  private String groupId = null;

  //End of my code

  private static final Logger LOG = LoggerFactory.getLogger(SpKafkaConsumer.class);

  public SpKafkaConsumer() {

  }

  public SpKafkaConsumer(KafkaTransportProtocol protocol, String topic, InternalEventProcessor<byte[]> eventProcessor) {
      this.protocol = protocol;
      this.topic = topic;
      this.eventProcessor = eventProcessor;
      this.isRunning = true;
  }


  // TODO backwards compatibility, remove later
  public SpKafkaConsumer(String kafkaUrl, String topic, InternalEventProcessor<byte[]> callback) {
    KafkaTransportProtocol protocol = new KafkaTransportProtocol();
    protocol.setKafkaPort(Integer.parseInt(kafkaUrl.split(":")[1]));
    protocol.setBrokerHostname(kafkaUrl.split(":")[0]);
    protocol.setTopicDefinition(new SimpleTopicDefinition(topic));

    try {
      this.connect(protocol, callback);
    } catch (SpRuntimeException e) {
      e.printStackTrace();
    }
  }

  @Override
  public void run() {
    //My code
    Properties props = getProperties();
    if (this.groupId != null){
      //If a groupId has been provided set it in the config
      props.replace(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
    } else{
      //If no groupId has been provided save the generated ID
      this.groupId = props.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
    }
    //End of my code
    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
    if (!patternTopic) {
      kafkaConsumer.subscribe(Collections.singletonList(topic));
    } else {
      topic = replaceWildcardWithPatternFormat(topic);
      kafkaConsumer.subscribe(Pattern.compile(topic), new ConsumerRebalanceListener() {
        @Override
        public void onPartitionsRevoked(Collection<TopicPartition> partitions) {
          // TODO
        }

        @Override
        public void onPartitionsAssigned(Collection<TopicPartition> partitions) {
          // TODO
        }
      });
    }
    //My code
    if (this.offset !=  null){
      //If an offset has been provided seek the offset to pick up processing from there
      //kafkaConsumer.seek(new TopicPartition(topic, 0), offset);
    }
    //End of my code
    while (isRunning) {
      ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(100);
      for (ConsumerRecord<String, byte[]> record : records) {
        byte[] rec = record.value();
        eventProcessor.onEvent(rec);
        //My code
        //save the offset each time an event is processed
        this.offset = record.offset();
        //End of my code
      }
    }
    this.isFinished = true;
    LOG.info("Closing Kafka Consumer.");
    kafkaConsumer.close();
    //My code
    synchronized (this){
      notifyAll();
    }
    //End of my code
  }

  private String replaceWildcardWithPatternFormat(String topic) {
    topic = topic.replaceAll("\\.", "\\\\.");
    return topic.replaceAll("\\*", ".*");
  }

  private Properties getProperties() {
    return new ConsumerConfigFactory(protocol).makeProperties();
  }

  @Override
  public void connect(KafkaTransportProtocol protocol, InternalEventProcessor<byte[]>
          eventProcessor)
          throws SpRuntimeException {
    LOG.info("Kafka consumer: Connecting to " + protocol.getTopicDefinition().getActualTopicName());
    if (protocol.getTopicDefinition() instanceof WildcardTopicDefinition) {
      this.patternTopic = true;
    }
    this.eventProcessor = eventProcessor;
    this.protocol = protocol;
    this.topic = protocol.getTopicDefinition().getActualTopicName();
    this.isRunning = true;

    Thread thread = new Thread(this);
    thread.start();
  }

  @Override
  public void disconnect() throws SpRuntimeException {
    LOG.info("Kafka consumer: Disconnecting from " + topic);
    this.isRunning = false;
    synchronized (this){
      try {
        wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
        System.out.println("Here we are");
      }
    }
  }

  @Override
  public Boolean isConnected() {
    return isRunning;
  }


  //My code
  @Override
  public synchronized String getConsumerState(boolean close) throws SpRuntimeException {
    if (close)
      try{
        close();
        return "\"GroupId:\"" + this.groupId;
      }catch (Exception e){
        e.printStackTrace();
      }
    return "\"Offset:\"" + offset;
  }

  @Override
  public void setConsumerState(String state) throws SpRuntimeException {
    System.out.println("\"Kafka consumer\"" + state);
    if (state.startsWith("\"GroupId:\"")){
      state = state.replaceFirst("\"GroupId:\"", "");
      this.groupId = state;
    }
    else if (state.startsWith("\"Offset:\"")){
      state = state.replaceFirst("\"Offset:\"", "");
      this.offset = Long.parseLong(state);
      this.protocol.setOffset(state);
    }
    else {
      throw new SpRuntimeException("Failed to restore Consumer state of Consumer with topic "+ this.topic);
    }
  }


  private void close() throws SpRuntimeException{
    disconnect();
    synchronized (this){
      try {
        if (!this.isFinished){
          System.out.println("Waiting...");
          wait();
          System.out.println("Done waiting");
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  //End of my code
}
