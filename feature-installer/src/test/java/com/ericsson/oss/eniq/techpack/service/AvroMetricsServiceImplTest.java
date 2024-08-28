package com.ericsson.oss.eniq.techpack.service;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.MockClock;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleConfig;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.junit.MockitoJUnitRunner;

import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class AvroMetricsServiceImplTest {

    private MockClock clock = new MockClock();
    private CompositeMeterRegistry composite = new CompositeMeterRegistry();
    private SimpleMeterRegistry simple = new SimpleMeterRegistry(SimpleConfig.DEFAULT, clock);

    @Before
    public void setup() {
        composite.counter("counter").increment();
    }

    @Test
    public void shouldCountMetrics() {
        Counter compositeCounter;
        compositeCounter = composite.counter("counter");
        compositeCounter.increment();
        assertThat(compositeCounter.count()).isEqualTo(0);
        composite.add(simple);
        compositeCounter.increment();
        assertThat(compositeCounter.count()).isEqualTo(1);
        assertThat(simple.get("counter").counter().count()).isEqualTo(1.0);
    }

    @Test
    public void ShouldRegistryCountBeforeMetricAdd() {
        composite.add(simple);
        composite.counter("counter").increment();
        assertThat(simple.get("counter").counter().count()).isEqualTo(1.0);
    }

    @Test
    public void shouldCheckCounterIncrement() {
        SimpleMeterRegistry simple = new SimpleMeterRegistry(SimpleConfig.DEFAULT, new MockClock());
        CompositeMeterRegistry registry = new CompositeMeterRegistry();
        registry.add(simple);
        registry.counter("counter").increment(2.0);
        assertThat(simple.get("counter").counter().count()).isEqualTo(2.0);
    }

    @Test
    public void shouldDisplayMetricsTimer() {

        SimpleMeterRegistry registry = new SimpleMeterRegistry();
        Timer timer = registry.timer("avroschema-schema");
        timer.record(() -> {
            try {
                TimeUnit.MILLISECONDS.sleep(1500);
            } catch (InterruptedException ignored) {
            }
        });

        timer.record(3000, TimeUnit.MILLISECONDS);
        Assert.assertTrue(2 == timer.count());
    }
}
