package com.benife.KafakaTest;

import com.fasterxml.jackson.databind.ObjectMapper;

public class JSONTest {
	
	public static void main(String[] args) throws Exception 
	{
		String json = "{" + 
				"    \"device_id\": \"awJo6rH...\"," + 
				"    \"last_event\": {" + 
				"      \"has_sound\": false," + 
				"      \"has_motion\": true," + 
				"      \"has_person\": false," + 
				"      \"start_time\": \"2016-12-29T00:00:00.000Z\"," + 
				"      \"end_time\": \"2016-12-29T18:42:00.000Z\"" + 
				"    }" + 
				"}";
		

    	ObjectMapper mapper = new ObjectMapper();
		DeviceA device = mapper.readValue(json, DeviceA.class);
		
		System.out.println(device.getDevice_id());
		System.out.println(device.getLast_event().getHas_sound());
		System.out.println(device.getLast_event().getHas_motion());
		System.out.println(device.getLast_event().getHas_person());
		System.out.println(device.getLast_event().getStart_time());
		System.out.println(device.getLast_event().getEnd_time());
		
		
    	
        //if(device.getDevice_id().equals("awJo6rH")){
				
	}

}


class DeviceA
{
	private String device_id;
	
	public static class Last_event {
		private boolean has_sound;
		private boolean has_motion;
		private boolean has_person;
		private String start_time;
		private String end_time;
		
		public boolean getHas_sound()
		{
			return has_sound;
		}
		
		public boolean getHas_motion()
		{
			return has_motion;
		}
		
		public boolean getHas_person()
		{
			return has_person;
		}
		
		public String getStart_time()
		{
			return start_time;
		}
		
		public String getEnd_time()
		{
			return end_time;
		}
		
		public void setHas_sound(boolean b)
		{
			has_sound = b;
		}
		
		public void setHas_motion(boolean b)
		{
			has_motion = b;
		}
		
		public void setHas_person(boolean b)
		{
			has_person= b;
		}
		
		public void setStart_time(String t)
		{
			start_time = t;
		}
		
		public void setEnd_time(String t)
		{
			end_time = t;
		}
	}
	
	public String getDevice_id()
	{
		return device_id;
	}
	
	public void setDevice_id(String t)
	{
		device_id = t;
	}
	
	private Last_event last_event;
	
	public Last_event getLast_event()
	{
		return last_event;
	}
	
	public void setLast_event(Last_event t)
	{
		last_event = t;
	}
}
