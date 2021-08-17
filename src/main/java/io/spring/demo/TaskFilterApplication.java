package io.spring.demo;

import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.util.Date;
import java.util.Locale;
import java.util.function.Function;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.spring.demo.entity.CallUsage;
import io.spring.demo.entity.DataUsage;
import io.spring.demo.entity.Plan;
import io.spring.demo.entity.User;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class TaskFilterApplication {


	public static void main(String[] args) {
		SpringApplication.run(TaskFilterApplication.class, args);
	}

	@Bean
	public Function<String, String> transform() {
		return  (payload) -> {
			ObjectMapper objectMapper = new ObjectMapper();
			JsonNode jsonNode = null;
			try {
				jsonNode = objectMapper.readTree(payload);
			}
			catch (JsonProcessingException e) {
				throw new IllegalStateException(e);
			}
			String result = "";
			String tableName = jsonNode.get("source").get("table").asText();
			if(tableName == null || jsonNode.get("after") == null) {
				return "";
			}
			if(tableName != null && tableName.equals("plans")) {
				Plan plan = new Plan(jsonNode.get("after").get("plan_id").asInt(-1),
						jsonNode.get("after").get("plan_name").asText(),
						jsonNode.get("after").get("data_price").asDouble(),
						jsonNode.get("after").get("data_price").asDouble());
				try {
					result = objectMapper.writeValueAsString(plan);
				}
				catch (JsonProcessingException e) {
					e.printStackTrace();
					result = "******  FAILED    " + payload;
				}
			}
			if(tableName != null && tableName.equals("users")) {
				User user = new User(jsonNode.get("after").get("id").asInt(-1),
						jsonNode.get("after").get("last_name").asText(),
						jsonNode.get("after").get("first_name").asText(),
						jsonNode.get("after").get("plan_id").asInt(-1));
				try {
					result = objectMapper.writeValueAsString(user);
				}
				catch (JsonProcessingException e) {
					e.printStackTrace();
					result = ""; //"******  FAILED    " + payload;
				}
			}
			System.out.println("******* >" +tableName);
			if(tableName != null && tableName.equals("call_usage")) {
				SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-DD hh:mm:ss", Locale.ENGLISH);
				try {
				CallUsage usage = new CallUsage(jsonNode.get("after").get("usage_in_minutes").asDouble(),
						new Date(jsonNode.get("after").get("date").asLong()),
						jsonNode.get("after").get("user_id").asInt(-1));
					result = objectMapper.writeValueAsString(usage);
				}
				catch (Exception e) {
					e.printStackTrace();
					result = ""; //"******  FAILED    " + payload;
				}
			}
			if(tableName != null && tableName.equals("data_usage")) {
				try {
					DataUsage usage = new DataUsage(jsonNode.get("after").get("usage_in_bytes").asDouble(),
							new Date(jsonNode.get("after").get("date").asLong()),
							jsonNode.get("after").get("user_id").asInt(-1));

					result = objectMapper.writeValueAsString(usage);

				}
				catch (Exception e) {
					e.printStackTrace();

					result = ""; //"******  FAILED    " + payload;
				}
			}
//			else {
//				result = "************ " + tableName + " ==> " + payload;
//			}
//			String result = null;
//			if(payload.contains("\"exitCode\":2")) {
//				JdbcTemplate template = new JdbcTemplate(dataSource);
//				template.execute("truncate table task_status");
//				result = "{\"name\":\""+ taskName + "\"}";
//			}
			return result;
		};
	}
}
