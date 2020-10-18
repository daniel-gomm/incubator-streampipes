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
import org.apache.kafka.clients.consumer.*;
import org.apache.kafka.common.PartitionInfo;
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
import java.time.Duration;
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

  private HashMap<Integer, Long> offsets = new HashMap<Integer, Long>();
  private HashMap<Integer, Long> startOffsets = new HashMap<>();
  private String groupId = null;
  private volatile boolean threadSuspended = false;


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
    Properties props = getProperties();
    props.replace("enable.auto.commit", "false");
    props.remove("auto.commit.interval.ms");
    if (this.groupId != null){
      //If a groupId has been provided set it in the config
      System.out.println("Registered with groupId: " + this.groupId);
      props.replace(ConsumerConfig.GROUP_ID_CONFIG, this.groupId);
    } else{
      //If no groupId has been provided save the generated ID
      this.groupId = props.get(ConsumerConfig.GROUP_ID_CONFIG).toString();
    }
    ConsumerRecord<String, byte[]> lastRecord = null;
    KafkaConsumer<String, byte[]> kafkaConsumer = new KafkaConsumer<>(props);
    ArrayList<TopicPartition> partitions = new ArrayList<>();
    for (Map.Entry<Integer, Long> e : startOffsets.entrySet()) {
      partitions.add(new TopicPartition(topic, e.getKey()));
    }
    try{
      if(!this.startOffsets.isEmpty()){
        kafkaConsumer.assign(partitions);
      } else if (!patternTopic) {
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
      //Reprocess missing events to restore current state
      if (!this.startOffsets.isEmpty()) {
        //Manually assign topic partitions
        Map<TopicPartition, Long> endOffsets = new HashMap<>();
        for(TopicPartition tp: partitions){
          endOffsets.put(tp, kafkaConsumer.position(tp));
        }
        //If an offset has been provided seek the offset to pick up processing from there
        for (Map.Entry<TopicPartition, Long> tp : endOffsets.entrySet()) {
          if (tp.getValue() != startOffsets.get(tp.getKey().partition()) + 1) {
            //Need to reprocess events
            long endOffset = tp.getValue();
            kafkaConsumer.seek(tp.getKey(), startOffsets.get(tp.getKey().partition()));
            while (true) {
              ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
              boolean brk = false;
              for (ConsumerRecord<String, byte[]> record : records) {
                lastRecord = record;
                if (record.offset() >= endOffset-0) {
                  brk = true;
                  break;
                }
                byte[] rec = record.value();
                eventProcessor.onEventReprocess(rec);
              }
              if (brk){
                //Process intermediary events
                for(ConsumerRecord<String, byte[]> record : records){
                  if(record.offset() < lastRecord.offset()) continue;
                  byte[] rec = record.value();
                  eventProcessor.onEvent(rec);
                  this.offsets.put(record.partition(), record.offset() + 1);
                }
                kafkaConsumer.commitAsync(Collections.singletonMap(
                        new TopicPartition(topic, lastRecord.partition()),
                        new OffsetAndMetadata(lastRecord.offset() +1)), null);
                break;
              }
            }
          }
        }
      }
      this.offsets = new HashMap<>();
      while (isRunning) {
        ConsumerRecords<String, byte[]> records = kafkaConsumer.poll(Duration.ofMillis(100));
        int i = 0;
        for (ConsumerRecord<String, byte[]> record : records) {
          byte[] rec = record.value();
          eventProcessor.onEvent(rec);
          lastRecord = record;
          this.offsets.put(record.partition(), record.offset() + 1);
          if(++i%20==0)
            kafkaConsumer.commitAsync(Collections.singletonMap(
                    new TopicPartition(topic, lastRecord.partition()),
                    new OffsetAndMetadata(lastRecord.offset() +1)), null);
        }
        if(!records.isEmpty() && lastRecord != null)
          kafkaConsumer.commitAsync();
        //My code -- check if paused, just paused after all records of last poll have been committed
        synchronized (this) {
          while (threadSuspended) {
            try {
              kafkaConsumer.unsubscribe();
              wait();
            } catch (InterruptedException e) {
              e.printStackTrace();
            }
            if (!isRunning) {
              break;
            }
            if(!threadSuspended){
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
            }
          }
        }
        //End of my code
      }
    }catch (Exception e){
      e.printStackTrace();
    }
    finally {
      this.isFinished = true;
      LOG.info("Closing Kafka Consumer.");
      if(lastRecord != null)
        kafkaConsumer.commitSync(Collections.singletonMap(new TopicPartition(topic, lastRecord.partition()), new OffsetAndMetadata(lastRecord.offset() +1)), Duration.ofMillis(1000L));
      kafkaConsumer.close();
      synchronized (this) {
        notifyAll();
      }
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
      if (this.threadSuspended = true){
        this.threadSuspended = false;
        notify();
      }
      try {
        wait();
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public Boolean isConnected() {
    return isRunning;
  }


  //My code
  private class KafkaState{
    String groupId;
    HashMap<Integer, Long> offsets;
  }

  @Override
  public synchronized String getConsumerState() throws SpRuntimeException {
    KafkaState ks = new KafkaState();
    ks.groupId = this.groupId;
    ks.offsets = this.offsets;
    String ret = new Gson().toJson(ks);
    return ret;
  }

  @Override
  public void setConsumerState(String state) throws SpRuntimeException {
    KafkaState kafkaState = new Gson().fromJson(state, KafkaState.class);
    if(kafkaState == null){
      throw new SpRuntimeException("Failed to restore Consumer state of Consumer with topic "+ this.topic);
    }
    this.groupId = kafkaState.groupId;
    this.startOffsets = kafkaState.offsets;
  }

  //TODO assess necessity
  private void close() throws SpRuntimeException{
    disconnect();
    synchronized (this){
      try {
        if (!this.isFinished){
          wait();
        }
      } catch (InterruptedException e) {
        e.printStackTrace();
      }
    }
  }

  @Override
  public void pause(){
    this.threadSuspended = true;
  }

  @Override
  public synchronized void resume(){
    this.threadSuspended = false;
    notify();
  }

  @Override
  public boolean isPaused() {
    return this.threadSuspended;
  }

}
