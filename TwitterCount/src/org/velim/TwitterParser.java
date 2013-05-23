package org.velim;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import com.eclipsesource.json.JsonArray;
import com.eclipsesource.json.JsonObject;
import com.eclipsesource.json.JsonValue;

public class TwitterParser {

	public static List<String> getHashTags(String tweet) {
		List<String> tags = new ArrayList<String>();

		try {
			JsonObject jsonObject = JsonObject.readFrom(tweet);

			JsonObject entities = jsonObject.get("entities").asObject();
			JsonArray hashtags = entities.get("hashtags").asArray();
				
			Iterator<JsonValue> itr = hashtags.iterator();
			while (itr.hasNext()) {
				JsonObject tag = itr.next().asObject();
				tags.add(tag.get("text").asString());
				System.out.println(tag.get("text").asString());
			}
			
		} catch (Exception e) {

		}
		return tags;
	}

}
