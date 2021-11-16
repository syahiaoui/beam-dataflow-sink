package org.zenika.com.beam.dataflow.sink.transforms;

import java.io.IOException;

import org.zenika.com.beam.dataflow.sink.utils.JSONUtils;
import org.zenika.com.beam.dataflow.sink.utils.ResourceUtils;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

public class Input {

	public static void main(String[] args) {
		ObjectMapper objectMapper = new ObjectMapper();

		// TODO Auto-generated method stub
		String inputString = "{\"data\":{\"personalNumber\":123456,\"firstName\":\"zenika\",\"lastName\":\"labs\",\"email\":\"xyz@zenika.com\",\"creationDate\":\"2021-11-04T17:53:04.175Z\"},\"processId\":\"7fefdc3f-5519-4c80-96da-e7be1b757707\",\"operationType\":\"MUTATION\",\"key\":\"123456_LABS\",\"processDate\":\"2021-11-15T18:29:28.150Z\"}";
		byte[] byteArrray = inputString.getBytes();
		try {
			JsonNode json = JSONUtils.ToJsonNode(byteArrray);
			final JsonNode valueAtPath = json.at("/data");
			System.out.println(">>>>>>>"+json);
			System.out.println(ResourceUtils.getDeadletterTableSchemaJson());
//
//			System.out.println(">>zz>>>>>"+valueAtPath.isMissingNode());
//			final JsonNode data = (valueAtPath != null || !valueAtPath.isMissingNode()) ? valueAtPath : null;
			//System.out.println(data);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

				
	}

}
