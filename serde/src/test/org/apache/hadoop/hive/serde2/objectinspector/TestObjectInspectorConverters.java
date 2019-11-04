/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.serde2.objectinspector;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import junit.framework.TestCase;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import parquet.example.data.simple.Primitive;

import javax.security.auth.login.Configuration;

/**
 * TestObjectInspectorConverters.
 *
 */
public class TestObjectInspectorConverters extends TestCase {

  public void testObjectInspectorConverters() throws Throwable {
    try {
      // Boolean
      Converter booleanConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);
      assertEquals("BooleanConverter", new BooleanWritable(false),
          booleanConverter.convert(Integer.valueOf(0)));
      assertEquals("BooleanConverter", new BooleanWritable(true),
          booleanConverter.convert(Integer.valueOf(1)));
      assertEquals("BooleanConverter", null, booleanConverter.convert(null));

      // Byte
      Converter byteConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableByteObjectInspector);
      assertEquals("ByteConverter", new ByteWritable((byte) 0), byteConverter
          .convert(Integer.valueOf(0)));
      assertEquals("ByteConverter", new ByteWritable((byte) 1), byteConverter
          .convert(Integer.valueOf(1)));
      assertEquals("ByteConverter", null, byteConverter.convert(null));

      // Short
      Converter shortConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableShortObjectInspector);
      assertEquals("ShortConverter", new ShortWritable((short) 0),
          shortConverter.convert(Integer.valueOf(0)));
      assertEquals("ShortConverter", new ShortWritable((short) 1),
          shortConverter.convert(Integer.valueOf(1)));
      assertEquals("ShortConverter", null, shortConverter.convert(null));

      // Int
      Converter intConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      assertEquals("IntConverter", new IntWritable(0), intConverter
          .convert(Integer.valueOf(0)));
      assertEquals("IntConverter", new IntWritable(1), intConverter
          .convert(Integer.valueOf(1)));
      assertEquals("IntConverter", null, intConverter.convert(null));

      // Long
      Converter longConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableLongObjectInspector);
      assertEquals("LongConverter", new LongWritable(0), longConverter
          .convert(Integer.valueOf(0)));
      assertEquals("LongConverter", new LongWritable(1), longConverter
          .convert(Integer.valueOf(1)));
      assertEquals("LongConverter", null, longConverter.convert(null));

      // Float
      Converter floatConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableFloatObjectInspector);
      assertEquals("LongConverter", new FloatWritable(0), floatConverter
          .convert(Integer.valueOf(0)));
      assertEquals("LongConverter", new FloatWritable(1), floatConverter
          .convert(Integer.valueOf(1)));
      assertEquals("LongConverter", null, floatConverter.convert(null));

      // Double
      Converter doubleConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableDoubleObjectInspector);
      assertEquals("DoubleConverter", new DoubleWritable(0), doubleConverter
          .convert(Integer.valueOf(0)));
      assertEquals("DoubleConverter", new DoubleWritable(1), doubleConverter
          .convert(Integer.valueOf(1)));
      assertEquals("DoubleConverter", null, doubleConverter.convert(null));

      // Text
      Converter textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaIntObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("0"), textConverter
          .convert(Integer.valueOf(0)));
      assertEquals("TextConverter", new Text("1"), textConverter
          .convert(Integer.valueOf(1)));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("hive"), textConverter
          .convert(new BytesWritable(new byte[]
              {(byte)'h', (byte)'i',(byte)'v',(byte)'e'})));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.writableStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("hive"), textConverter
	  .convert(new Text("hive")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("hive"), textConverter
	  .convert(new String("hive")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      textConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaHiveDecimalObjectInspector,
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      assertEquals("TextConverter", new Text("100.001"), textConverter
	  .convert(HiveDecimal.create("100.001")));
      assertEquals("TextConverter", null, textConverter.convert(null));

      // Binary
      Converter baConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.javaStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
      assertEquals("BAConverter", new BytesWritable(new byte[]
          {(byte)'h', (byte)'i',(byte)'v',(byte)'e'}),
          baConverter.convert("hive"));
      assertEquals("BAConverter", null, baConverter.convert(null));

      baConverter = ObjectInspectorConverters.getConverter(
          PrimitiveObjectInspectorFactory.writableStringObjectInspector,
          PrimitiveObjectInspectorFactory.writableBinaryObjectInspector);
      assertEquals("BAConverter", new BytesWritable(new byte[]
          {(byte)'h', (byte)'i',(byte)'v',(byte)'e'}),
          baConverter.convert(new Text("hive")));
      assertEquals("BAConverter", null, baConverter.convert(null));

      // Union
      ArrayList<String> fieldNames = new ArrayList<String>();
      fieldNames.add("firstInteger");
      fieldNames.add("secondString");
      fieldNames.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors = new ArrayList<ObjectInspector>();
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectors
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      ArrayList<String> fieldNames2 = new ArrayList<String>();
      fieldNames2.add("firstString");
      fieldNames2.add("secondInteger");
      fieldNames2.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectors2 = new ArrayList<ObjectInspector>();
      fieldObjectInspectors2
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectors2
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectors2
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      Converter unionConverter0 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors),
          ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors2));

      Object convertedObject0 = unionConverter0.convert(new StandardUnionObjectInspector.StandardUnion((byte)0, 1));
      List<String> expectedObject0 = new ArrayList<String>();
      expectedObject0.add("1");

      assertEquals(expectedObject0, convertedObject0);

      Converter unionConverter1 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors),
		  ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors2));

      Object convertedObject1 = unionConverter1.convert(new StandardUnionObjectInspector.StandardUnion((byte)1, "1"));
      List<Integer> expectedObject1 = new ArrayList<Integer>();
      expectedObject1.add(1);

      assertEquals(expectedObject1, convertedObject1);

      Converter unionConverter2 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors),
          ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectors2));

      Object convertedObject2 = unionConverter2.convert(new StandardUnionObjectInspector.StandardUnion((byte)2, true));
      List<Boolean> expectedObject2 = new ArrayList<Boolean>();
      expectedObject2.add(true);

      assertEquals(expectedObject2, convertedObject2);

      // Union (extra fields)
      ArrayList<String> fieldNamesExtra = new ArrayList<String>();
      fieldNamesExtra.add("firstInteger");
      fieldNamesExtra.add("secondString");
      fieldNamesExtra.add("thirdBoolean");
      ArrayList<ObjectInspector> fieldObjectInspectorsExtra = new ArrayList<ObjectInspector>();
      fieldObjectInspectorsExtra
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);
      fieldObjectInspectorsExtra
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectorsExtra
          .add(PrimitiveObjectInspectorFactory.javaBooleanObjectInspector);

      ArrayList<String> fieldNamesExtra2 = new ArrayList<String>();
      fieldNamesExtra2.add("firstString");
      fieldNamesExtra2.add("secondInteger");
      ArrayList<ObjectInspector> fieldObjectInspectorsExtra2 = new ArrayList<ObjectInspector>();
      fieldObjectInspectorsExtra2
          .add(PrimitiveObjectInspectorFactory.javaStringObjectInspector);
      fieldObjectInspectorsExtra2
          .add(PrimitiveObjectInspectorFactory.javaIntObjectInspector);

      Converter unionConverterExtra = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectorsExtra),
          ObjectInspectorFactory.getStandardUnionObjectInspector(fieldObjectInspectorsExtra2));

      Object convertedObjectExtra = unionConverterExtra.convert(new StandardUnionObjectInspector.StandardUnion((byte)2, true));
      List<Object> expectedObjectExtra = new ArrayList<Object>();
      expectedObjectExtra.add(null);

      assertEquals(expectedObjectExtra, convertedObjectExtra); // we should get back null

    } catch (Throwable e) {
      e.printStackTrace();
      throw e;
    }

  }

  public void testStructSchemaEvolution() throws Throwable {

    HiveConf conf = new HiveConf();
    conf.setBoolVar(HiveConf.ConfVars.HIVE_STRUCT_SCHEMA_CONVERSION_BY_NAME, true);

    //case 1 : input has more fields than output
    List<String> inputFields = Arrays.asList("col1", "col2", "col3");
    List<ObjectInspector> inputOIs = Arrays.asList(new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector});

    List<String> outputFields = Arrays.asList("col2");
    List<ObjectInspector> outputOIs = Arrays.asList(new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector});

    Converter structConverter = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardStructObjectInspector(inputFields, inputOIs),
            ObjectInspectorFactory.getStandardStructObjectInspector(outputFields, outputOIs), conf);

    ArrayList<Object> struct1In = new ArrayList<>();
    struct1In.add(1);
    struct1In.add("two");
    struct1In.add(true);

    ArrayList struct1Out = (ArrayList) structConverter.convert(struct1In);
    assertEquals(1, struct1Out.size());
    assertEquals("two", struct1Out.get(0));


    //case 2 : input has less fields than output
    List<String> inputFields2 = Arrays.asList("col1", "col2");
    List<ObjectInspector> inputOIs2 = Arrays.asList(new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector});

    List<String> outputFields2 = Arrays.asList("col0", "col1", "col2");
    List<ObjectInspector> outputOIs2 = Arrays.asList(new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaBooleanObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector});

    Converter structConverter2 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardStructObjectInspector(inputFields2, inputOIs2),
            ObjectInspectorFactory.getStandardStructObjectInspector(outputFields2, outputOIs2), conf);
    ArrayList<Object> struct2In = new ArrayList<Object>();
    struct2In.add(1);
    struct2In.add("two");

    ArrayList struct2Out = (ArrayList) structConverter2.convert(struct2In);
    assertEquals(3, struct2Out.size());
    assertEquals(null, struct2Out.get(0));
    assertEquals(1, struct2Out.get(1));
    assertEquals("two", struct2Out.get(2));

    //case 3 : input and output have the same fields with a different ordering
    List<String> inputFields3 = Arrays.asList("col1", "col2");
    List<ObjectInspector> inputOIs3 = Arrays.asList(new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaIntObjectInspector,
            PrimitiveObjectInspectorFactory.javaStringObjectInspector});

    List<String> outputFields3 = Arrays.asList("col2", "col1");
    List<ObjectInspector> outputOIs3 = Arrays.asList(new ObjectInspector[]{
            PrimitiveObjectInspectorFactory.javaStringObjectInspector,
            PrimitiveObjectInspectorFactory.javaIntObjectInspector});

    Converter structConverter3 = ObjectInspectorConverters.getConverter(ObjectInspectorFactory.getStandardStructObjectInspector(inputFields3, inputOIs3),
            ObjectInspectorFactory.getStandardStructObjectInspector(outputFields3, outputOIs3), conf);
    ArrayList<Object> struct3In = new ArrayList<>();
    struct3In.add(1);
    struct3In.add("two");

    ArrayList struct3Out = (ArrayList) structConverter3.convert(struct3In);
    assertEquals(2, struct3Out.size());
    assertEquals("two", struct3Out.get(0));
    assertEquals(1, struct3Out.get(1));
  }

  public void testGetConvertedOI() throws Throwable {
    // Try with types that have type params
    PrimitiveTypeInfo varchar5TI =
        (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("varchar(5)");
    PrimitiveTypeInfo varchar10TI =
        (PrimitiveTypeInfo) TypeInfoFactory.getPrimitiveTypeInfo("varchar(10)");
    PrimitiveObjectInspector varchar5OI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(varchar5TI);
    PrimitiveObjectInspector varchar10OI =
        PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(varchar10TI);

    // output OI should have varchar type params
    PrimitiveObjectInspector poi = (PrimitiveObjectInspector)
        ObjectInspectorConverters.getConvertedOI(varchar10OI, varchar5OI);
    VarcharTypeInfo vcParams = (VarcharTypeInfo) poi.getTypeInfo();
    assertEquals("varchar length doesn't match", 5, vcParams.getLength());
  }
}