package com.ericsson.oss.eniq.techpack.service;

import com.ericsson.oss.eniq.techpack.constants.TechPackConstant;
import com.ericsson.oss.eniq.techpack.service.interfaces.AvroMetricsService;
import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;

@Service
@Slf4j
public class AvroMetricsServiceImpl implements AvroMetricsService {

    @Autowired
    private MeterRegistry meterRegistry;

    @Override
    public Counter increment(String tagValue) {
        Map<String, Counter> counters = new HashMap<>();
        Counter counter = counters.get(tagValue);
        if (counter == null) {
            counter = Counter.builder(TechPackConstant.AVRO_COUNTER_NAME)
                    .tags(TechPackConstant.AVRO_COUNTER_TAG_NAME, tagValue)
                    .register(meterRegistry);
            counters.put(tagValue, counter);
        }
        counter.increment();

        return counter;
    }

    @Override
    public Timer getTimer(String tagValue) {
        Map<String, Timer> timers = new HashMap<>();
        Timer timer = timers.get(tagValue);
        if (timer == null) {
            timer = Timer.builder(TechPackConstant.AVRO_TIMER_NAME)
                    .tags(TechPackConstant.AVRO_TIMER_TAG_NAME, tagValue)
                    .register(meterRegistry);
            timers.put(tagValue, timer);
        }
        return timer;
    }
}
