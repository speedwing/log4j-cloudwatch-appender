
package com.github.speedwing.log4j.cloudwatch.appender;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.log4j.Level;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;
import org.junit.jupiter.api.Test;
import software.amazon.awssdk.regions.Region;
import java.util.Optional;
import java.util.UUID;

public class CloudwatchAppenderTest {
    @Test
    public void unspecifiedAwsRegion() {
        Logger logger = makeLogger();
        LoggingEvent event = makeLoggingEvent(logger);
        CloudwatchAppender appender = new CloudwatchAppender();
        appender.setRegion(Optional.empty());
        appender.activateOptions();
        appender.append(event);
        appender.close();
    }

    @Test
    public void explicitAwsRegion() {
        Logger logger = makeLogger();
        LoggingEvent event = makeLoggingEvent(logger);
        CloudwatchAppender appender = new CloudwatchAppender();
        appender.setRegion(Optional.of(Region.AWS_US_GOV_GLOBAL.id()));
        appender.activateOptions();
        appender.append(event);
        appender.close();
    }

    @Test
    public void bogusAwsRegion()  {
        Logger logger = makeLogger();
        LoggingEvent event = makeLoggingEvent(logger);
        CloudwatchAppender appender = new CloudwatchAppender();
        appender.setRegion(Optional.of("bogus-region"));
        appender.activateOptions();
        appender.append(event);
        appender.close();
    }

    private Logger makeLogger() {
        return Logger.getLogger(UUID.randomUUID().toString());
    }

    private LoggingEvent makeLoggingEvent(Logger logger) {
        return new LoggingEvent(logger.getName(), logger, Level.INFO, "Hello World " + System.currentTimeMillis(), null);
    }

}


