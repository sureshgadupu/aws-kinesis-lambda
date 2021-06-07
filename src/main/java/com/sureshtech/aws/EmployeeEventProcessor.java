package com.sureshtech.aws;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EmployeeEventProcessor {
	
	private static final Logger logger =  LoggerFactory.getLogger(EmployeeEventProcessor.class);
	
	public int processEmployeeEvents(List<EmployeeEvent> empEvents) {
		
		 for (EmployeeEvent employeeEvent : empEvents) {
	    	   logger.info("Emp event -> Id :  "+ employeeEvent.getId() +" , Name :"+  employeeEvent.getName() +" , Address : "+employeeEvent.getAddress() +" , Salary :"+ employeeEvent.getSalary());
	     }
		 return empEvents.size();
		
	}

}
