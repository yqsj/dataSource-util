package com.yiqi.HBase.util;



import lombok.extern.slf4j.Slf4j;

import java.io.*;

@SuppressWarnings("unchecked")
@Slf4j
public class ByteUtils implements java.io.Serializable {
  private static final long serialVersionUID = 6233544976754880837L;
  /**
   * 图片压缩.
   * @param _bytes
   * @return
   * @throws IOException
   */
  public static byte[] picCompress(byte[] _bytes) throws IOException {
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(out);
    gzip.write(_bytes);
    gzip.close();
    byte[] bytes = out.toByteArray();
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (bytes[i] - 1);
    }
    return bytes;
  }

  /**
   * 图片解压缩.
   * @param _bytes
   * @return
   * @throws IOException
   */
  public static byte[] picUncompress(byte[] _bytes) throws IOException {
    if (null == _bytes || 0 == _bytes.length) { return _bytes; }
    for (int i = 0; i < _bytes.length; i++) {
      _bytes[i] = (byte) (_bytes[i] + 1);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayInputStream in = new ByteArrayInputStream(_bytes);
    java.util.zip.GZIPInputStream gunzip = new java.util.zip.GZIPInputStream(in);
    byte[] buffer = new byte[128];
    int n = 0;
    while ((n = gunzip.read(buffer)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toByteArray();
  }

  /**
   * 字符串压缩.
   * @param str
   * @return
   * @throws IOException
   */
  public static byte[] compress(String str) throws IOException {
    if (null == str || 0 == str.length()) { return new byte[] {}; }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    java.util.zip.GZIPOutputStream gzip = new java.util.zip.GZIPOutputStream(out);
    gzip.write(str.getBytes("UTF-8"));
    gzip.close();
    byte[] bytes = out.toByteArray();
    for (int i = 0; i < bytes.length; i++) {
      bytes[i] = (byte) (bytes[i] - 1);
    }
    return bytes;
  }

  /**
   * 字符串解压缩.
   * @param _bytes
   * @return
   * @throws IOException
   */
  public static String uncompress(byte[] _bytes) throws IOException {
    if (null == _bytes || 0 == _bytes.length) { return ""; }
    for (int i = 0; i < _bytes.length; i++) {
      _bytes[i] = (byte) (_bytes[i] + 1);
    }
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ByteArrayInputStream in = new ByteArrayInputStream(_bytes);
    java.util.zip.GZIPInputStream gunzip = new java.util.zip.GZIPInputStream(in);
    byte[] buffer = new byte[128];
    int n = 0;
    while ((n = gunzip.read(buffer)) >= 0) {
      out.write(buffer, 0, n);
    }
    return out.toString("UTF-8");
  }

  /**
   * 对象序列化.
   * @param obj
   * @return
   */
  public static byte[] getbytes(Object obj) {
    byte[] res = null;
    ByteArrayOutputStream out = new ByteArrayOutputStream();
    ObjectOutputStream ops = null;
    try {
      ops = new ObjectOutputStream(out);
      ops.writeObject(obj);
      ops.close();
      res = out.toByteArray();
    } catch (IOException e) {
      log.error("IOException occurs:{}", e);
    }
    return res;
  }

  /**
   * 对象反对象序列化.
   * @param clazz
   * @param bytes
   * @return
   */
  public static <T> T fromBytes(Class<T> clazz, byte[] bytes) {
    T instance = null;
    try {
      ByteArrayInputStream in = new ByteArrayInputStream(bytes);
      ObjectInputStream ops;
      instance = clazz.newInstance();
      ops = new ObjectInputStream(in);
      instance = (T) ops.readObject();
    } catch (IOException e) {
      log.error("IOException occurs:{}", e);
    } catch (ClassNotFoundException e) {
      log.error("ClassNotFoundException occurs:{}", e);
    } catch (InstantiationException e) {
      log.error("InstantiationException occurs:{}", e);
    } catch (IllegalAccessException e) {
      log.error("IllegalAccessException occurs:{}", e);
    } catch (Exception e) {
      log.error("Exception occurs:{}", e);
    }
    return instance;
  }
}
