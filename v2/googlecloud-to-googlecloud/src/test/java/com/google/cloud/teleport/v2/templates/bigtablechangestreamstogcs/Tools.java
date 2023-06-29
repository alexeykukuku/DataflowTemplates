package com.google.cloud.teleport.v2.templates.bigtablechangestreamstogcs;

import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Base64;
import javax.tools.Tool;

public class Tools {
  private Tools() {
    // don't create
  }


  public static String bbToString(ByteBuffer val) {
    if (val == null) {
      return null;
    }
    return new String(val.array(), Charset.defaultCharset());
  }


  public static String bbToBase64String(ByteBuffer val) {
    if (val == null) {
      return null;
    }
    return Base64.getEncoder().encodeToString(val.array());
  }
}
