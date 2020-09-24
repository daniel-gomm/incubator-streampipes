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

package org.apache.streampipes.model.util;

import org.apache.streampipes.model.output.*;
import org.apache.streampipes.model.staticproperty.*;
import org.apache.streampipes.model.grounding.MqttTransportProtocol;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.streampipes.model.ApplicationLink;
import org.apache.streampipes.model.SpDataSet;
import org.apache.streampipes.model.SpDataStream;
import org.apache.streampipes.model.base.NamedStreamPipesEntity;
import org.apache.streampipes.model.connect.adapter.AdapterDescription;
import org.apache.streampipes.model.connect.adapter.AdapterSetDescription;
import org.apache.streampipes.model.connect.adapter.AdapterStreamDescription;
import org.apache.streampipes.model.connect.adapter.GenericAdapterSetDescription;
import org.apache.streampipes.model.connect.adapter.GenericAdapterStreamDescription;
import org.apache.streampipes.model.connect.adapter.SpecificAdapterSetDescription;
import org.apache.streampipes.model.connect.adapter.SpecificAdapterStreamDescription;
import org.apache.streampipes.model.graph.DataProcessorDescription;
import org.apache.streampipes.model.graph.DataSinkDescription;
import org.apache.streampipes.model.grounding.JmsTransportProtocol;
import org.apache.streampipes.model.grounding.KafkaTransportProtocol;
import org.apache.streampipes.model.grounding.SimpleTopicDefinition;
import org.apache.streampipes.model.grounding.TopicDefinition;
import org.apache.streampipes.model.grounding.TransportFormat;
import org.apache.streampipes.model.grounding.TransportProtocol;
import org.apache.streampipes.model.grounding.WildcardTopicDefinition;
import org.apache.streampipes.model.grounding.WildcardTopicMapping;
import org.apache.streampipes.model.quality.Accuracy;
import org.apache.streampipes.model.quality.EventPropertyQualityDefinition;
import org.apache.streampipes.model.quality.EventPropertyQualityRequirement;
import org.apache.streampipes.model.quality.MeasurementCapability;
import org.apache.streampipes.model.quality.MeasurementObject;
import org.apache.streampipes.model.quality.Precision;
import org.apache.streampipes.model.quality.Resolution;
import org.apache.streampipes.model.schema.Enumeration;
import org.apache.streampipes.model.schema.EventProperty;
import org.apache.streampipes.model.schema.EventPropertyList;
import org.apache.streampipes.model.schema.EventPropertyNested;
import org.apache.streampipes.model.schema.EventPropertyPrimitive;
import org.apache.streampipes.model.schema.QuantitativeValue;
import org.apache.streampipes.model.schema.ValueSpecification;
import org.apache.streampipes.model.template.BoundPipelineElement;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class Cloner {

  private final Logger LOG = LoggerFactory.getLogger(Cloner.class);

  public OutputStrategy outputStrategy(OutputStrategy other) {
    if (other instanceof KeepOutputStrategy) {
      return new KeepOutputStrategy((KeepOutputStrategy) other);
    } else if (other instanceof FixedOutputStrategy) {
      return new FixedOutputStrategy((FixedOutputStrategy) other);
    } else if (other instanceof ListOutputStrategy) {
      return new ListOutputStrategy((ListOutputStrategy) other);
    } else if (other instanceof CustomOutputStrategy) {
      return new CustomOutputStrategy((CustomOutputStrategy) other);
    } else if (other instanceof TransformOutputStrategy) {
      return new TransformOutputStrategy((TransformOutputStrategy) other);
    } else if (other instanceof CustomTransformOutputStrategy) {
      return new CustomTransformOutputStrategy((CustomTransformOutputStrategy) other);
    } else if (other instanceof UserDefinedOutputStrategy) {
      return new UserDefinedOutputStrategy((UserDefinedOutputStrategy) other);
    } else {
      return new AppendOutputStrategy((AppendOutputStrategy) other);
    }
  }

  public StaticProperty staticProperty(StaticProperty o) {
    if (o instanceof FreeTextStaticProperty) {
      return new FreeTextStaticProperty((FreeTextStaticProperty) o);
    } else if (o instanceof RuntimeResolvableOneOfStaticProperty) {
      return new RuntimeResolvableOneOfStaticProperty((RuntimeResolvableOneOfStaticProperty) o);
    } else if (o instanceof RuntimeResolvableAnyStaticProperty) {
      return new RuntimeResolvableAnyStaticProperty((RuntimeResolvableAnyStaticProperty) o);
    } else if (o instanceof OneOfStaticProperty) {
      return new OneOfStaticProperty((OneOfStaticProperty) o);
    } else if (o instanceof RemoteOneOfStaticProperty) {
      return new RemoteOneOfStaticProperty((RemoteOneOfStaticProperty) o);
    } else if (o instanceof MappingPropertyNary) {
      return new MappingPropertyNary((MappingPropertyNary) o);
    } else if (o instanceof DomainStaticProperty) {
      return new DomainStaticProperty((DomainStaticProperty) o);
    } else if (o instanceof AnyStaticProperty) {
      return new AnyStaticProperty((AnyStaticProperty) o);
    } else if (o instanceof CollectionStaticProperty) {
      return new CollectionStaticProperty((CollectionStaticProperty) o);
    } else if (o instanceof MatchingStaticProperty) {
      return new MatchingStaticProperty((MatchingStaticProperty) o);
    } else if (o instanceof MappingPropertyUnary) {
      return new MappingPropertyUnary((MappingPropertyUnary) o);
    } else if (o instanceof StaticPropertyGroup) {
      return new StaticPropertyGroup((StaticPropertyGroup) o);
    } else if (o instanceof StaticPropertyAlternatives) {
      return new StaticPropertyAlternatives((StaticPropertyAlternatives) o);
    } else if (o instanceof SecretStaticProperty) {
      return new SecretStaticProperty((SecretStaticProperty) o);
    } else if (o instanceof FileStaticProperty) {
      return new FileStaticProperty((FileStaticProperty) o);
    } else if (o instanceof CodeInputStaticProperty) {
      return new CodeInputStaticProperty((CodeInputStaticProperty) o);
    } else if (o instanceof ColorPickerStaticProperty) {
      return new ColorPickerStaticProperty((ColorPickerStaticProperty) o);
    } else {
      return new StaticPropertyAlternative((StaticPropertyAlternative) o);
    }

  }

  public List<TransportProtocol> protocols(List<TransportProtocol> protocols) {
    return protocols.stream().map(this::protocol).collect(Collectors.toList());
  }

  public TransportProtocol protocol(TransportProtocol protocol) {
    if (protocol instanceof KafkaTransportProtocol) {
      return new KafkaTransportProtocol((KafkaTransportProtocol) protocol);
    } else if (protocol instanceof JmsTransportProtocol){
      return new JmsTransportProtocol((JmsTransportProtocol) protocol);
    } else if (protocol instanceof MqttTransportProtocol) {
      return new MqttTransportProtocol((MqttTransportProtocol) protocol);
    } else {
      LOG.error("Could not clone protocol of type {}", protocol.getClass().getCanonicalName());
      return protocol;
    }
  }

  public List<WildcardTopicMapping> wildcardTopics(List<WildcardTopicMapping> topicMappings) {
    if (topicMappings == null) {
      return new ArrayList<>();
    } else {
      return topicMappings.stream().map(t -> new WildcardTopicMapping(t)).collect(Collectors.toList());
    }
  }

  public EventProperty property(EventProperty o) {
    if (o instanceof EventPropertyPrimitive) {
      return new EventPropertyPrimitive((EventPropertyPrimitive) o);
    } else if (o instanceof EventPropertyList) {
      return new EventPropertyList((EventPropertyList) o);
    } else {
      return new EventPropertyNested((EventPropertyNested) o);
    }
  }

  public ValueSpecification valueSpecification(ValueSpecification o) {
    if (o instanceof QuantitativeValue) {
      return new QuantitativeValue((QuantitativeValue) o);
    } else {
      return new Enumeration((Enumeration) o);
    }
  }

  public EventPropertyQualityRequirement qualityreq(EventPropertyQualityRequirement o) {
    // TODO Auto-generated method stub
    return null;
  }

  public EventPropertyQualityDefinition qualitydef(EventPropertyQualityDefinition o) {
    if (o instanceof Accuracy) {
      return new Accuracy((Accuracy) o);
    } else if (o instanceof Precision) {
      return new Precision((Precision) o);
    } else {
      return new Resolution((Resolution) o);
    }
  }

  public List<SpDataStream> seq(List<SpDataStream> spDataStreams) {
    return spDataStreams.stream().map(s -> mapSequence(s)).collect(Collectors.toList());
  }

  public List<SpDataStream> streams(List<SpDataStream> spDataStreams) {
    return spDataStreams.stream().map(s -> new SpDataStream(s)).collect(Collectors.toList());
  }

  public SpDataStream mapSequence(SpDataStream seq) {
    if (seq instanceof SpDataSet) {
      return new SpDataSet((SpDataSet) seq);
    } else {
      return new SpDataStream(seq);
    }
  }

  public SpDataStream stream(SpDataStream other) {
    return new SpDataStream(other);
  }

  public List<OutputStrategy> strategies(List<OutputStrategy> outputStrategies) {
    if (outputStrategies != null) {
      return outputStrategies.stream().map(o -> outputStrategy(o)).collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  public List<StaticProperty> staticProperties(
          List<StaticProperty> staticProperties) {
    if (staticProperties != null) {
      return staticProperties.stream().map(o -> staticProperty(o)).collect(Collectors.toList());
    } else {
      return new ArrayList<>();
    }
  }

  public List<TransportFormat> transportFormats(
          List<TransportFormat> transportFormats) {
    return transportFormats.stream().map(t -> new TransportFormat(t)).collect(Collectors.toList());
  }

  public List<EventProperty> properties(List<EventProperty> eventProperties) {
    return eventProperties.stream().map(o -> new Cloner().property(o)).collect(Collectors.toList());
  }

  public List<TransformOperation> transformOperations(List<TransformOperation> transformOperations) {
    return transformOperations.stream().map(o -> new TransformOperation(o)).collect(Collectors.toList());
  }

  public List<EventPropertyQualityRequirement> reqEpQualitities(
          List<EventPropertyQualityRequirement> requiresEventPropertyQualities) {
    return requiresEventPropertyQualities.stream().map(o -> new Cloner().qualityreq(o)).collect(Collectors.toList());
  }

  public List<EventPropertyQualityDefinition> provEpQualities(
          List<EventPropertyQualityDefinition> eventPropertyQualities) {
    return eventPropertyQualities.stream().map(o -> new Cloner().qualitydef(o)).collect(Collectors.toList());
  }

  public List<Option> options(List<Option> options) {
    return options.stream().map(o -> new Option(o)).collect(Collectors.toList());
  }

  public List<SupportedProperty> supportedProperties(
          List<SupportedProperty> supportedProperties) {
    return supportedProperties.stream().map(s -> new SupportedProperty(s)).collect(Collectors.toList());
  }

  public List<String> epaTypes(List<String> epaTypes) {
    return epaTypes;
  }

  public List<String> ecTypes(List<String> ecTypes) {
    return ecTypes;
  }

  public List<MeasurementCapability> mc(
          List<MeasurementCapability> measurementCapability) {
    return measurementCapability.stream().map(m -> new MeasurementCapability(m)).collect(Collectors.toList());
  }

  public List<MeasurementObject> mo(List<MeasurementObject> measurementObject) {
    return measurementObject.stream().map(m -> new MeasurementObject(m)).collect(Collectors.toList());
  }

  public List<ApplicationLink> al(List<ApplicationLink> applicationLinks) {
    return applicationLinks.stream().map(m -> new ApplicationLink(m)).collect(Collectors.toList());
  }

  public TopicDefinition topicDefinition(TopicDefinition topicDefinition) {
    if (topicDefinition instanceof SimpleTopicDefinition) {
      return new SimpleTopicDefinition((SimpleTopicDefinition) topicDefinition);
    } else {
      return new WildcardTopicDefinition((WildcardTopicDefinition) topicDefinition);
    }
  }

  public List<BoundPipelineElement> boundPipelineElements(List<BoundPipelineElement> boundPipelineElements) {
    return boundPipelineElements
            .stream()
            .map(BoundPipelineElement::new)
            .collect(Collectors.toList());
  }

  public List<NamedStreamPipesEntity> cloneDescriptions(List<NamedStreamPipesEntity> pipelineElementDescriptions) {
    return pipelineElementDescriptions
            .stream()
            .map(pe -> cloneDescription(pe))
            .collect(Collectors.toList());
  }

  private NamedStreamPipesEntity cloneDescription(NamedStreamPipesEntity pe) {
    if (pe instanceof SpDataSet) {
      return new SpDataSet((SpDataSet) pe);
    } else if (pe instanceof SpDataStream) {
      return new SpDataStream((SpDataStream) pe);
    } else if (pe instanceof DataProcessorDescription) {
      return new DataProcessorDescription((DataProcessorDescription) pe);
    } else if (pe instanceof DataSinkDescription) {
      return new DataSinkDescription((DataSinkDescription) pe);
    } else {
      LOG.error("Description is of unknown type: " + pe.getClass().getCanonicalName());
      return pe;
    }
  }

  public List<PropertyRenameRule> renameRules(List<PropertyRenameRule> renameRules) {
    return renameRules
            .stream()
            .map(PropertyRenameRule::new)
            .collect(Collectors.toList());
  }

  public AdapterDescription adapterDescription(AdapterDescription ad) {
    if (ad instanceof GenericAdapterSetDescription) {
      return new GenericAdapterSetDescription((GenericAdapterSetDescription) ad);
    } else if (ad instanceof GenericAdapterStreamDescription) {
      return new GenericAdapterStreamDescription((GenericAdapterStreamDescription) ad);
    } else if (ad instanceof SpecificAdapterSetDescription) {
      return new SpecificAdapterSetDescription((AdapterSetDescription) ad);
    } else if (ad instanceof SpecificAdapterStreamDescription) {
      return new SpecificAdapterStreamDescription((AdapterStreamDescription) ad);
    } else {
      LOG.error("Could not clone adapter description of type: " +ad.getClass().getCanonicalName());
      return ad;
    }
  }
}
