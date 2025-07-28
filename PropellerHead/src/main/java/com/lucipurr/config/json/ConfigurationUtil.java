/* (C) Lucipurr 69@420 */
package com.lucipurr.config.json;

import com.fasterxml.jackson.annotation.JsonAutoDetect.Visibility;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.FileWriter;
import java.io.IOException;
import java.io.StringWriter;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.HashMap;
import java.util.Map;
import lombok.extern.slf4j.Slf4j;
import org.json.JSONObject;
import org.springframework.core.io.ClassPathResource;
import org.springframework.util.FileCopyUtils;

@Slf4j
class ConfigurationUtil {

  /**
   * @param json
   * @return
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonGenerationException
   */
  public static <T> byte[] objectToByte(T obj)
      throws JsonGenerationException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, obj);
    return writer.toString().getBytes();
  }

  /**
   * @param obj
   * @return
   * @throws JsonGenerationException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static <T> String objectToString(T obj)
      throws JsonGenerationException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    StringWriter writer = new StringWriter();
    mapper.writeValue(writer, obj);
    return writer.toString();
  }

  /**
   * @param data
   * @return
   * @throws IOException
   * @throws JsonMappingException
   * @throws JsonParseException
   */
  public static <T> T byteToObject(byte[] data, Class<T> type)
      throws JsonParseException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(data, type);
  }

  /**
   * @param data
   * @param type
   * @return
   * @throws JsonParseException
   * @throws JsonMappingException
   * @throws IOException
   */
  public static <T> T stringToObject(String data, Class<T> type)
      throws JsonParseException, JsonMappingException, IOException {
    ObjectMapper mapper = new ObjectMapper();
    return mapper.readValue(data, type);
  }

  /**
   * @param key
   * @param value
   * @param filename
   * @param appendFlag
   * @throws IOException
   */
  public static void writeOrUpdateJson(
      String key, String value, String filename, boolean appendFlag) throws IOException {
    JSONObject obj;
    if (getJsonObject(filename) != null) {
      obj = getJsonObject(filename);
    } else {
      obj = new JSONObject();
    }
    obj.put(key, value);
    writeFile(obj, filename, appendFlag);
  }

  /**
   * @param obj
   * @param filename
   * @param appendFlag
   * @throws IOException
   */
  public static void writeFile(JSONObject obj, String filename, boolean appendFlag)
      throws IOException {
    FileWriter file = new FileWriter(filename, appendFlag);
    ObjectMapper mapper = new ObjectMapper();
    mapper.setVisibility(PropertyAccessor.FIELD, Visibility.ANY);
    mapper.writeValue(file, obj);
  }

  /**
   * @param key
   * @param filename
   * @return
   * @throws IOException
   */
  public static String readFile(String key, String filename) {
    JSONObject obj = (JSONObject) getJsonObject(filename);
    return obj != null ? (String) obj.get(key) : null;
  }

  /**
   * @param filename
   * @return
   * @throws IOException
   */
  public static JSONObject getJsonObject(String filename) {
    synchronized (cache) {
      if (!cache.containsKey(filename)) {
        readToCache(filename);
        listenForFileChange(filename);
      }
    }
    return cache.get(filename);
  }

  public static JSONObject getJsonObject(String filename, boolean fromFile) {
    if (!fromFile) return getJsonObject(filename);
    else {
      synchronized (cache) {
        readToCache(filename);
        listenForFileChange(filename);
      }
      return cache.get(filename);
    }
  }

  private static void listenForFileChange(String filename) {
    // TODO: Listen to changes for runtime modifications to file
  }

  private static void readToCache(String filename) {
    byte[] encoded = null;
    try {
      encoded = Files.readAllBytes(Paths.get(filename));
    } catch (Exception e) {
      log.error("Error reading file, trying with classpath");
      try {
        encoded =
            Files.readAllBytes(
                Paths.get(
                    ConfigurationUtil.class.getClassLoader().getResource(filename).getPath()));
      } catch (Exception ie) {
        log.error("Error reading file, trying with ClassPathResource using inputStream");
        try {
          ClassPathResource resrc = new ClassPathResource(filename);
          encoded = FileCopyUtils.copyToByteArray(resrc.getInputStream());
        } catch (Exception exc) {
          log.error("Error reading file, ignoring but this may cause problems later", exc);
          return;
        }
      }
    }
    String data = new String(encoded);
    if (data.length() == 0) return;
    JSONObject obj = (JSONObject) new JSONObject(data).get("map");
    cache.put(filename, obj);
  }

  private static Map<String, JSONObject> cache = new HashMap<>();
}
