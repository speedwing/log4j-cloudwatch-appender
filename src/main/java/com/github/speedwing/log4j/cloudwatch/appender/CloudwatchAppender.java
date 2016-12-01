package com.github.speedwing.log4j.cloudwatch.appender;

import com.amazonaws.services.logs.AWSLogsClient;
import com.amazonaws.services.logs.model.*;
import org.apache.log4j.AppenderSkeleton;
import org.apache.log4j.Layout;
import org.apache.log4j.spi.LoggingEvent;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicReference;

public class CloudwatchAppender extends AppenderSkeleton {

    private AWSLogsClient awsLogsClient;

    private AtomicReference<String> lastSequenceToken = new AtomicReference<>();

    private String logGroupName;

    private String logStreamName;

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

    @Override
    protected void append(LoggingEvent event) {

        String lineEvent = layout.format(event);

        InputLogEvent inputLogEvent = new InputLogEvent().withTimestamp(new Date().getTime()).withMessage(lineEvent);
        List<InputLogEvent> list = new ArrayList<>();
        list.add(inputLogEvent);
        PutLogEventsRequest putLogEventsRequest = new PutLogEventsRequest(
                logGroupName,
                logStreamName, list);

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

    @Override
    public void close() {

    }

    @Override
    public boolean requiresLayout() {
        return true;
    }

    @Override
    public void activateOptions() {
        super.activateOptions();
        this.awsLogsClient = new AWSLogsClient();
        initializeCloudwatchResources();
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
            CreateLogStreamRequest createLogStreamRequest = new CreateLogStreamRequest().withLogGroupName(logGroupName).withLogStreamName(logStreamName);
            awsLogsClient.createLogStream(createLogStreamRequest);
        }

    }

}
