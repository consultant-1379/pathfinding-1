package com.ericsson.oss.eniq.techpack.service.interfaces;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Timer;

public interface AvroMetricsService {

   public Counter increment(String tagValue);
   public Timer getTimer(String tagValue);
}
