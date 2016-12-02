# log4j-cloudwatch-appender

Log4j Cloudwatch Appender is a simple to use Log4j Appender that allows you to stream your application logs directly into your [AWS Cloudwatch Logs](http://docs.aws.amazon.com/AmazonCloudWatch/latest/logs/WhatIsCloudWatchLogs.html).

In order to optimise network usage and mitigate throttling issues with AWS Cloudwatch, the actual sending of logs may be deferred.
 
The central (and unique!) class of this library is `com.github.speedwing.log4j.cloudwatch.appender.CloudwatchAppender` and extends `org.apache.log4j.AppenderSkeleton` from which it inherits the following features:
* threshold filtering 
* general filters
* lifecycle management

## Programmatic Configuration
 
The `CloudwatchAppender` can be configure either via properties file (see next paragraph) or programmatically in your codebase.

In order to setup the `CloudwatchAppender` in your codebase it's necessary to add these imports:

```java
import com.github.speedwing.log4j.cloudwatch.appender.CloudwatchAppender;
import org.apache.log4j.Layout;
import org.apache.log4j.Logger;
import org.apache.log4j.PatternLayout;
import org.apache.log4j.Priority;
```

Now we can now initialise and configure the appender: 

```java
PatternLayout layout = new org.apache.log4j.PatternLayout();
layout.setConversionPattern("%d [%t] %-5p %c - %m%n");
CloudwatchAppender cloudwatchAppender = new CloudwatchAppender(layout, "MyLogName", "MyStreamName");
cloudwatchAppender.setThreshold(Priority.WARN);
Logger.getRootLogger().addAppender(cloudwatchAppender);
```

## Configuration via log4j.properties

A much more classic approach in configuring the appender is via the classic `log4j.properties`.

An example below:

```properties
log4j.appender.CloudW=com.github.speedwing.log4j.cloudwatch.appender.CloudwatchAppender
log4j.appender.CloudW.layout=org.apache.log4j.PatternLayout
log4j.appender.CloudW.layout.ConversionPattern=%d [%t] %-5p %c - %m%n
log4j.appender.CloudW.logGroupName=MyLogGroupName
log4j.appender.CloudW.logStreamName=MyLogStreamName
log4j.appender.CloudW.queueLength=2048
log4j.appender.CloudW.messagesBatchSize=512

log4j.rootLogger=INFO, CloudW
```

As you can see alongside `logGroupName` and `logStreamName` attributes, there are a couple of optional parameters `queueLength` and `messagesBatchSize`.
These parameters are used to influence the way `CloudwatchAppender` communicate to AWS Cloutwatch.

### The queueLength parameter
Since throttling can be a real issue when using the AWS Cloudwatch Logs service it's important to mitigate the RPM against it. For this reason an intermediate buffer
has been introduced in order to gather a few log entries together and then send them over the wire in one go.
 
The parameter `queueLength` sets the maximum number of log entries that can be *buffered* at any one time by the `CloudwatchAppender`, subsequent log messages will be discarded.

A separate thread will be polling for messages the buffer and send them over to AWS Cloudwatch.

The default value for `queueLength` is 1024

### The messagesBatchSize parameter
It's the maximum number of log entries to send in one go to AWS Cloudwatch. It's important to pick this large enough to avoid frequent AWS Cloudwatch Logs api calls.

The default value for `messagesBatchSize` is 128

### The log4j.debug flag
The `CloudwatchAppender` honors the `log4j.debug` flag and will print extra logging info on the standard out / err.
  