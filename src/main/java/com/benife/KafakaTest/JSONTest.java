package com.benife.KafakaTest;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONTest {
	
	public static void main(String[] args) throws Exception 
	{
		String json = "{" 
				+ "\"header\" : {" 
		        + "\"command\" : \"abStatus\","
		        + "\"sourceDeviceId\" : \"P_22340\","
		        + "\"targetDeviceId\" : \"G_12345\""
		        + "},"
		        + "\"data\" : {"
		        + "\"errorCode\" : 101"
		        + "}"
				+ "}";
		
		

    	ObjectMapper mapper = new ObjectMapper();
		DeviceA device = mapper.readValue(json, DeviceA.class);
		
		System.out.println(device.getHeader().getCommand());
		System.out.println(device.getHeader().getSourceDeviceId());
		System.out.println(device.getHeader().getTargetDeviceId());
		System.out.println(device.getData().getErrorCode());
		
				
	}

}


class DeviceA
{	
	public static class Header {
		private String command;
		private String sourceDeviceId;
		private String targetDeviceId;
		
		public String getCommand()
		{
			return command;
		}
		
		public String getSourceDeviceId()
		{
			return sourceDeviceId;
		}
		
		public String getTargetDeviceId()
		{
			return targetDeviceId;
		}
		
		public void setCommand(String s)
		{
			command = s;
		}
		
		public void setSourceDeviceId(String s)
		{
			sourceDeviceId = s;
		}
		
		public void setTargetDeviceId(String s)
		{
			targetDeviceId= s;
		}
	}
	
	public static class Data {
		private String errorCode;
		
		public String getErrorCode()
		{
			return errorCode;
		}
		
				
		public void setErrorCode(String s)
		{
			errorCode = s;
		}
	}
		
	private Header header;
	private Data data;
	
	public Header getHeader()
	{
		return header;
	}
	
	public void setHeader(Header h)
	{
		header = h;
	}
	
	public Data getData()
	{
		return data;
	}
	
	public void setData(Data d)
	{
		data = d;
	}
}
