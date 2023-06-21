package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import com.google.gson.TypeAdapter;
import com.google.gson.stream.JsonReader;
import com.google.gson.stream.JsonToken;
import com.google.gson.stream.JsonWriter;
import java.io.IOException;

public class CharSequenceTypeAdapter extends TypeAdapter<CharSequence> {
  @Override
  public void write(JsonWriter out, CharSequence value) throws IOException {
    if (value == null) {
      out.nullValue();
    } else {
      // Assumes that value complies with CharSequence.toString() contract
      out.value(value.toString());
    }
  }

  @Override
  public CharSequence read(JsonReader in) throws IOException {
    if (in.peek() == JsonToken.NULL) {
      // Skip the JSON null
      in.skipValue();
      return null;
    } else {
      return in.nextString();
    }
  }
}
