package com.uber.data.kafka.connect.utils;

import java.io.IOException;
import java.util.Map;

public final class SerializationUtils {

  public static Map<?, ?> parseMapField(boolean isKey, Map<?, ?> map, String field) throws IOException {
    Object fieldValue = map.get(field);
    if (fieldValue == null)
      throw new MissingFieldException(isKey, field);

    if (!(fieldValue instanceof Map))
      throw new InvalidFieldTypeException(isKey, field, "map", fieldValue);

    return (Map<?, ?>) fieldValue;
  }

  public static String parseStringField(boolean isKey, Map<?, ?> map, String field) throws IOException {
    String result = parseOptionalStringField(isKey, map, field);

    if (result == null)
      throw new MissingFieldException(isKey, field);

    return result;
  }

  public static String parseOptionalStringField(boolean isKey, Map<?, ?> map, String field) throws IOException {
    Object fieldValue = map.get(field);
    if (fieldValue == null)
      return null;

    if (!(fieldValue instanceof String))
      throw new InvalidFieldTypeException(isKey, field, "string", fieldValue);

    return (String) fieldValue;
  }

  public static Number parseNumberField(boolean isKey, Map<?, ?> map, String field) throws IOException {
    Number result = parseOptionalNumberField(isKey, map, field);

    if (result == null)
      throw new MissingFieldException(isKey, field);

    return result;
  }

  public static Number parseOptionalNumberField(boolean isKey, Map<?, ?> map, String field) throws IOException {
    Object fieldValue = map.get(field);
    if (fieldValue == null)
      return null;

    if (!(fieldValue instanceof Number))
      throw new InvalidFieldTypeException(isKey, field, "string", fieldValue);

    return (Number) fieldValue;
  }

  public static class MissingFieldException extends IOException {
    public MissingFieldException(boolean isKey, String field) {
      super(
          "record has invalid " + (isKey ? "key" : "value")
              + ": missing '" + field + "' field"
      );
    }
  }

  public static class InvalidFieldTypeException extends IOException {
    public InvalidFieldTypeException(boolean isKey, String field, String expectedType, Object actualValue) {
      super(
          "record has invalid " + (isKey ? "key" : "value")
              + ": field '" + field + "' should be a(n) " + expectedType
              + "; was " + (actualValue == null ? "null" : actualValue.getClass().getSimpleName())
      );
    }
  }

}
