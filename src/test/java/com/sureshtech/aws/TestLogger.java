package com.sureshtech.aws;



import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.amazonaws.services.lambda.runtime.LambdaLogger;

public class TestLogger implements LambdaLogger {
	
	private static final Logger logger =  LoggerFactory.getLogger(TestLogger.class);
	@Override
	public void log(String message) {
		// TODO Auto-generated method stub
		logger.info(message);
	}

	@Override
	public void log(byte[] message) {
		// TODO Auto-generated method stub
		logger.info(new String(message));
	}

}
