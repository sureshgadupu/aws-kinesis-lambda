package com.sureshtech.aws;

import com.amazonaws.services.lambda.runtime.ClientContext;
import com.amazonaws.services.lambda.runtime.CognitoIdentity;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class TestContext implements Context{
	
	private String awsRequestId = "EXAMPLE";
    private ClientContext clientContext;
    private String functionName = "EXAMPLE";
    private CognitoIdentity identity;
    private String logGroupName = "EXAMPLE";
    private String logStreamName = "EXAMPLE";
    private LambdaLogger logger = new TestLogger();
    private int memoryLimitInMB = 128;
    private int remainingTimeInMillis = 15000;

    public String getAwsRequestId() {
        return awsRequestId;
    }

    public void setAwsRequestId(String value) {
        awsRequestId = value;
    }

    
    public ClientContext getClientContext() {
        return clientContext;
    }

    public void setClientContext(ClientContext value) {
        clientContext = value;
    }

   
    public String getFunctionName() {
        return functionName;
    }

    public void setFunctionName(String value) {
        functionName = value;
    }

    public CognitoIdentity getIdentity() {
        return identity;
    }

    public void setIdentity(CognitoIdentity value) {
        identity = value;
    }

    public String getLogGroupName() {
        return logGroupName;
    }

    public void setLogGroupName(String value) {
        logGroupName = value;
    }

    public String getLogStreamName() {
        return logStreamName;
    }

    public void setLogStreamName(String value) {
        logStreamName = value;
    }

    public LambdaLogger getLogger() {
        return logger;
    }

    public void setLogger(LambdaLogger value) {
        logger = value;
    }

    public int getMemoryLimitInMB() {
        return memoryLimitInMB;
    }

    public void setMemoryLimitInMB(int value) {
        memoryLimitInMB = value;
    }

    public int getRemainingTimeInMillis() {
        return remainingTimeInMillis;
    }

    public void setRemainingTimeInMillis(int value) {
        remainingTimeInMillis = value;
    }

	@Override
	public String getFunctionVersion() {
		// TODO Auto-generated method stub
		return new String("$LATEST");
	}

	@Override
	public String getInvokedFunctionArn() {
		// TODO Auto-generated method stub
		return new String("arn:aws:lambda:us-east-2:12345678:function:AwsKinesisLambda");
	}

}
