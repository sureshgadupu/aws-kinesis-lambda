package com.sureshtech.aws;

import static java.nio.ByteBuffer.wrap;
import static java.util.Collections.singletonList;
import static net.andreinc.mockneat.unit.objects.Reflect.reflect;
import static net.andreinc.mockneat.unit.types.Ints.ints;
import static org.mockito.Mockito.mock;

import java.io.ByteArrayOutputStream;
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

@ExtendWith(MockitoExtension.class)
public class AwsKinesisLambdaTest {
	
	private ObjectMapper objectMapper = new ObjectMapper();
	

	private AwsKinesisLambda lambda;

	
	private EmployeeEventProcessor employeeEventProcessor;
	
	 

    @Test
    public void testLambdaHandleRequest() throws Exception {
    	
    	lambda = new AwsKinesisLambda();
    	
    	Context testContext = new TestContext();
    	
    	employeeEventProcessor = mock(EmployeeEventProcessor.class);
    	
    	lambda.setEmpEventProcessor(employeeEventProcessor);

    	Mockito.when(employeeEventProcessor.processEmployeeEvents(ArgumentMatchers.anyList())).thenReturn(4);
    	
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
    	
    	MockNeat m = MockNeat.threadLocal();
    	Reflect<EmployeeEvent> empEventBuilder = reflect(EmployeeEvent.class)    											
    											 .field("name", m.names()  )
    											 .field("address", m.strings().size(10).get())
    											 .useDefaults(true);
    	
    	List<EmployeeEvent>  empEventList = empEventBuilder.list(4).get();
    	
    	byte[] data = objectMapper.writeValueAsBytes(empEventList);
    	
    	if(gzip) {
    		
    		ByteArrayOutputStream byteOut = new ByteArrayOutputStream();
    		
    		GZIPOutputStream gzipOut = new GZIPOutputStream(byteOut);
    		gzipOut.write(data);
    		gzipOut.close();
    		
    		data = byteOut.toByteArray();
    	}
    	
    	KinesisEvent.Record record =  new KinesisEvent.Record();
    	record.setData(wrap(data));
    	
    	KinesisEvent.KinesisEventRecord kinesisEventRecord = new KinesisEvent.KinesisEventRecord();
    	kinesisEventRecord.setEventID("shardId-1234567");
    	kinesisEventRecord.setKinesis(record);
    	
    	KinesisEvent kinesisEvent = new KinesisEvent();
    	kinesisEvent.setRecords(singletonList(kinesisEventRecord));
    	
    	return kinesisEvent;
    }
}
