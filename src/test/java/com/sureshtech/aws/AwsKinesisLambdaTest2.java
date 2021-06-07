package com.sureshtech.aws;

import static java.nio.ByteBuffer.wrap;
import static java.util.Collections.singletonList;
import static net.andreinc.mockneat.unit.objects.Reflect.reflect;
import static net.andreinc.mockneat.unit.types.Ints.ints;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.zip.GZIPOutputStream;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;

import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;
import com.fasterxml.jackson.databind.ObjectMapper;

import net.andreinc.mockneat.MockNeat;
import net.andreinc.mockneat.unit.objects.Reflect;


public class AwsKinesisLambdaTest2 {
	
	private ObjectMapper objectMapper = new ObjectMapper();
	

	private AwsKinesisLambda lambda;

	
	private EmployeeEventProcessor employeeEventProcessor;
	
	 

    @Test
    public void testLambdaHandleRequest() throws Exception {
    	
    	lambda = new AwsKinesisLambda();
    	
    	Context testContext = new TestContext();
    	
    	employeeEventProcessor = mock(EmployeeEventProcessor.class);
    	
    	lambda.setEmpEventProcessor(employeeEventProcessor);

    	Mockito.when(employeeEventProcessor.processEmployeeEvents(ArgumentMatchers.anyList())).thenReturn(3);
    	
        lambda.handleRequest(createKinesisEvent(false), testContext);

        Mockito.verify(employeeEventProcessor, Mockito.times(1)).processEmployeeEvents(ArgumentMatchers.anyList());
    }
    
   @Test
    public void testLambdaLocaltest() throws Exception {
    	
    	lambda = new AwsKinesisLambda();
    	
    	Context testContext = new TestContext();
    	
    	employeeEventProcessor = new EmployeeEventProcessor();
    	
    	lambda.setEmpEventProcessor(employeeEventProcessor);

    	    	
        lambda.handleRequest(createKinesisEvent(false), testContext);

    }
    
    private KinesisEvent createKinesisEvent(boolean gzip) throws Exception {
    	
    	KinesisEvent kinesisEvent = new KinesisEvent();
    	
    	String str = "["+
    	  "{"+
    	    "\"id\": 1234,"+
    	    "\"name\": \"Suresh\","+
    	    "\"address\": \"Hyderabad\"," +
    	    "\"salary\": 523.45" +
    	  "},"+
	    	"{"+
	  	    "\"id\": 6567,"+
	  	    "\"name\": \"Naresh\","+
	  	    "\"address\": \"Delhi\"," +
	  	    "\"salary\": 893.45" +
	  	  "},"+
		  "{"+
		    "\"id\": 9367,"+
		    "\"name\": \"Mahesh\","+
		    "\"address\": \"Hyderabad\"," +
		    "\"salary\": 456.90" +
		  "}"+
    	  
    	"]";
    	
    	
    	
    	final ByteBuffer buffer = ByteBuffer.wrap(str.getBytes());
    	
    	KinesisEvent.Record record =  new KinesisEvent.Record();
    	record.setData(buffer);
    	record.setKinesisSchemaVersion("1.0");
    	record.setPartitionKey("partition-1");
    	record.setSequenceNumber("1234");
    	
    	
    	KinesisEvent.KinesisEventRecord kinesisEventRecord = new KinesisEvent.KinesisEventRecord();
    	kinesisEventRecord.setEventID("shardId-1234567");
    	kinesisEventRecord.setEventSourceARN("arn:aws:kinesis:example");
    	kinesisEventRecord.setEventName("aws:kinesis:record");
    	kinesisEventRecord.setKinesis(record);
    	
    	
    	kinesisEvent.setRecords(singletonList(kinesisEventRecord));
    	
    	return kinesisEvent;
    }
}
