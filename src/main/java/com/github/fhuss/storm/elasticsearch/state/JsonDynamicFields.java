package com.github.fhuss.storm.elasticsearch.state;


import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.IOException;
import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.*;


public class JsonDynamicFields extends Object implements Serializable{

  protected Map<String, String> properties = new HashMap<>();

  protected static final ObjectMapper mapper = new ObjectMapper();

  public byte[] serialize() throws IOException {
    return mapper.writeValueAsBytes(properties);
  }

  public void deserialize(byte[] bytes) throws IOException {
    properties = mapper.readValue(bytes, new TypeReference<Map<String, String>>(){});
  }

  @Override
  public boolean equals(Object obj) {
    JsonDynamicFields df = (JsonDynamicFields)obj;
    if (!this.properties.equals(df.properties)) {
      return false;
    }

    return true;
  }

}
