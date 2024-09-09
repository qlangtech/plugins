package com.qlangtech.tis.plugin.dolphinscheduler.task;

import org.apache.dolphinscheduler.plugin.task.api.parameters.AbstractParameters;

/**
 * @author: 百岁（baisui@qlangtech.com）
 **/
public class TISDatasyncParameters extends AbstractParameters {
    private String destinationLocationArn;
    private String sourceLocationArn;
    private String name;
    private String cloudWatchLogGroupArn;
    @Override
    public boolean checkParameters() {
        return true;
    }

    public String getDestinationLocationArn() {
        return this.destinationLocationArn;
    }

    public void setDestinationLocationArn(String destinationLocationArn) {
        this.destinationLocationArn = destinationLocationArn;
    }

    public String getSourceLocationArn() {
        return sourceLocationArn;
    }

    public void setSourceLocationArn(String sourceLocationArn) {
        this.sourceLocationArn = sourceLocationArn;
    }

    public String getName() {
        return this.name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getCloudWatchLogGroupArn() {
        return cloudWatchLogGroupArn;
    }

    public void setCloudWatchLogGroupArn(String cloudWatchLogGroupArn) {
        this.cloudWatchLogGroupArn = cloudWatchLogGroupArn;
    }
}
