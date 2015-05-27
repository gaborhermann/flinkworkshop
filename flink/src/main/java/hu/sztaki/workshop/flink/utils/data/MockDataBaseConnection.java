package hu.sztaki.workshop.flink.utils.data;

import java.util.HashMap;
import java.util.Map;

import org.apache.flink.api.java.tuple.Tuple3;

public class MockDataBaseConnection {

	private Map<String, String> userToCountry;
	private transient boolean initialized = false;

	public MockDataBaseConnection() {
		userToCountry = new HashMap<>(StaticData.USERS.size());

		for (Tuple3<String, String, String> user : StaticData.USERS) {
			userToCountry.put(user.f0, user.f1);
		}

		initialized = true;
	}

	public String getCountryByUser(String user) throws Exception {
		if (!initialized) {
			throw new Exception("Connection to database not initialized");
		}
		return userToCountry.get(user);
	}
}
