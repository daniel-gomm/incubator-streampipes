package org.streampipes.pe.mixed.flink.samples.statistics.window;

import org.streampipes.model.impl.graph.SepaInvocation;
import org.streampipes.wrapper.params.binding.EventProcessorBindingParams;

import java.util.concurrent.TimeUnit;

/**
 * Created by riemer on 20.04.2017.
 */
public class StatisticsSummaryParametersWindow extends EventProcessorBindingParams {

  private String valueToObserve;
  private String timestampMapping;
  private String groupBy;
  private Long timeWindowSize;
  private TimeUnit timeUnit;



  public StatisticsSummaryParametersWindow(SepaInvocation graph) {
    super(graph);
  }

  public StatisticsSummaryParametersWindow(SepaInvocation graph, String valueToObserve,
                                           String timestampMapping, String groupBy, Long
                                                   timeWindowSize, TimeUnit timeUnit) {
    super(graph);
    this.valueToObserve = valueToObserve;
    this.timestampMapping = timestampMapping;
    this.groupBy = groupBy;
    this.timeWindowSize = timeWindowSize;
    this.timeUnit = timeUnit;
  }

  public String getValueToObserve() {
    return valueToObserve;
  }

  public String getGroupBy() {
    return groupBy;
  }

  public Long getTimeWindowSize() {
    return timeWindowSize;
  }

  public TimeUnit getTimeUnit() {
    return timeUnit;
  }

  public String getTimestampMapping() {
    return timestampMapping;
  }
}
