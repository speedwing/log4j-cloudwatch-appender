package com.github.speedwing.log4j.cloudwatch.appender;


import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.*;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import static java.util.stream.Collectors.toList;

public class CloudwatchAppender extends AppenderSkeleton {

    private LinkedBlockingQueue<LoggingEvent> loggingEventsQueue;

    private AWSLogsClient awsLogsClient;

    private AtomicReference<String> lastSequenceToken = new AtomicReference<>();

    private String logGroupName;

    private String logStreamName;

    private int queueLength = 1024;

    private int messagesBatchSize = 128;

    private AtomicBoolean cloudwatchAppenderInitialised = new AtomicBoolean(false);

    public CloudwatchAppender() {
        super();
    }

    public CloudwatchAppender(Layout layout, String logGroupName, String logStreamName) {
        super();
        this.setLayout(layout);
        this.setLogGroupName(logGroupName);
        this.setLogStreamName(logStreamName);
        this.activateOptions();
    }

    public void setLogGroupName(String logGroupName) {
        this.logGroupName = logGroupName;
    }

    public void setLogStreamName(String logStreamName) {
        this.logStreamName = logStreamName;
    }

    public void setQueueLength(int queueLength) {
        this.queueLength = queueLength;
    }

    public void setMessagesBatchSize(int messagesBatchSize) {
        this.messagesBatchSize = messagesBatchSize;
    }

    @Override
    protected void append(LoggingEvent event) {
        if (cloudwatchAppenderInitialised.get()) {
            loggingEventsQueue.offer(event);
        } else {
            // just do nothing
        }
    }

    private synchronized void sendMessages() {
        LoggingEvent polledLoggingEvent;

        List<LoggingEvent> loggingEvents = new ArrayList<>();

        try {

            while ((polledLoggingEvent = loggingEventsQueue.poll()) != null && loggingEvents.size() <= messagesBatchSize) {
                loggingEvents.add(polledLoggingEvent);
            }

            List<InputLogEvent> inputLogEvents = loggingEvents.stream()
                    .map(loggingEvent -> new InputLogEvent().withTimestamp(loggingEvent.getTimeStamp()).withMessage(layout.format(loggingEvent)))
                    .collect(toList());

            if (!inputLogEvents.isEmpty()) {

                PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest(
                        logGroupName,
                        logStreamName,
                        inputLogEvents);

                try {
                    putLogEventsRequest.setSequenceToken(lastSequenceToken.get());
                    PutLogEventsResult result = awsLogsClient.putLogEvents(putLogEventsRequest);
                    lastSequenceToken.set(result.getNextSequenceToken());
                } catch (InvalidSequenceTokenException invalidSequenceTokenException) {
                    putLogEventsRequest.setSequenceToken(invalidSequenceTokenException.getExpectedSequenceToken());
                    PutLogEventsResult result = awsLogsClient.putLogEvents(putLogEventsRequest);
                    lastSequenceToken.set(result.getNextSequenceToken());
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

    }

    @Override
    public void close() {
        while (!loggingEventsQueue.isEmpty()) {
            this.sendMessages();
        }
    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        this.awsLogsClient = new AWSLogsClient();
        loggingEventsQueue = new LinkedBlockingQueue<>(queueLength);
        try {
            initializeCloudwatchResources();
            initCloudwatchDaemon();
            cloudwatchAppenderInitialised.set(true);
        } catch (Exception e) {
            Logger.getRootLogger().error("Could not initialise Cloudwatch Logs for LogGroupName: " + logGroupName + " and LogStreamName: " + logStreamName, e);
        }
    }

    private void initCloudwatchDaemon() {
        new Thread(() -> {
            while (true) {
                try {
                    if (loggingEventsQueue.size() > 0) {
                        sendMessages();
                    }
                    Thread.currentThread().sleep(20L);
                } catch (InterruptedException e) {
                }
            }
        }).start();
    }

    private void initializeCloudwatchResources() {

        DescribeLogGroupsRequest describeLogGroupsRequest = new DescribeLogGroupsRequest();
        describeLogGroupsRequest.setLogGroupNamePrefix(logGroupName);

        Optional<LogGroup> logGroupOptional = awsLogsClient
                .describeLogGroups(describeLogGroupsRequest)
                .getLogGroups()
                .stream()
                .filter(logGroup -> logGroup.getLogGroupName().equals(logGroupName))
                .findFirst();

        if (!logGroupOptional.isPresent()) {
            CreateLogGroupRequest createLogGroupRequest = new CreateLogGroupRequest().withLogGroupName(logGroupName);
            awsLogsClient.createLogGroup(createLogGroupRequest);
        }

        DescribeLogStreamsRequest describeLogStreamsRequest = new DescribeLogStreamsRequest().withLogGroupName(logGroupName).withLogStreamNamePrefix(logStreamName);

        Optional<LogStream> logStreamOptional = awsLogsClient
                .describeLogStreams(describeLogStreamsRequest)
                .getLogStreams()
                .stream()
                .filter(logStream -> logStream.getLogStreamName().equals(logStreamName))
                .findFirst();

        if (!logStreamOptional.isPresent()) {
            Logger.getLogger(this.getClass()).info("About to create LogStream: " + logStreamName + "in LogGroup: " + logGroupName);
            CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest().withLogGroupName(logGroupName).withLogStreamName(logStreamName);
            awsLogsClient.createLogStream(createLogStreamRequest);
        }

    }

}
