/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.io.parquet.serde;

// BEGIN Failed patch imports blindly added--don't hate me.
//
import java.io.IOException;
import java.net.URI;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.parquet.convert.ParquetSchemaReader;
import org.apache.hadoop.hive.ql.io.parquet.convert.ParquetToHiveSchemaConverter;
//
// END blind imports


import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.AbstractSerDe;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.SerDeSpec;
import org.apache.hadoop.hive.serde2.SerDeStats;
import org.apache.hadoop.hive.serde2.io.ParquetHiveRecord;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.Writable;
import parquet.hadoop.ParquetOutputFormat;
import parquet.hadoop.ParquetWriter;

/**
 *
 * A ParquetHiveSerDe for Hive (with the deprecated package mapred)
 *
 */
@SerDeSpec(schemaProps = {serdeConstants.LIST_COLUMNS, serdeConstants.LIST_COLUMN_TYPES,
        ParquetOutputFormat.COMPRESSION})
public class ParquetHiveSerDe extends AbstractSerDe {
  private static final Log LOG = LogFactory.getLog(ParquetHiveSerDe.class);

  public static final Text MAP_KEY = new Text("key");
  public static final Text MAP_VALUE = new Text("value");
  public static final Text MAP = new Text("map");
  public static final Text ARRAY = new Text("bag");
  public static final Text LIST = new Text("list");

  // Map precision to the number bytes needed for binary conversion.
  public static final int PRECISION_TO_BYTE_COUNT[] = new int[38];
  static {
    for (int prec = 1; prec <= 38; prec++) {
      // Estimated number of bytes needed.
      PRECISION_TO_BYTE_COUNT[prec - 1] = (int)
          Math.ceil((Math.log(Math.pow(10, prec) - 1) / Math.log(2) + 1) / 8);
    }
  }

  private SerDeStats stats;
  private ObjectInspector objInspector;

  private enum LAST_OPERATION {
    SERIALIZE,
    DESERIALIZE,
    UNKNOWN
  }

  private LAST_OPERATION status;
  private long serializedSize;
  private long deserializedSize;

  private ParquetHiveRecord parquetRow;

  public ParquetHiveSerDe() {
    parquetRow = new ParquetHiveRecord();
    stats = new SerDeStats();
  }

  @Override
  public final void initialize(final Configuration conf, final Properties tbl) throws SerDeException {

    final TypeInfo rowTypeInfo;
    List<String> columnNames;
    List<TypeInfo> columnTypes;
    // Get column names and sort order
    final String columnNameProperty = tbl.getProperty(serdeConstants.LIST_COLUMNS);
    final String columnTypeProperty = tbl.getProperty(serdeConstants.LIST_COLUMN_TYPES);

    if (columnNameProperty.length() == 0 && columnTypeProperty.length() == 0) {
        final String locationProperty = tbl.getProperty("location", null);
        Path parquetFile = locationProperty != null ? getParquetFile(conf,
            new Path(locationProperty)) : null;

      if (parquetFile == null) {
        /**
         * Attempt to determine hive schema failed, but can not throw
         * an exception, as Hive calls init on the serde during
         * any call, including calls to update the serde properties, meaning
         * if the serde is in a bad state, there is no way to update that state.
         */
        LOG.error("Failed to create hive schema for the parquet backed table.\n" +
            "Either provide schema for table,\n" +
            "OR make sure that external table's path has at least one parquet file with required " +
            "metadata");
        columnNames = new ArrayList<String>();
        columnTypes = new ArrayList<TypeInfo>();
      } else {
        StructTypeInfo structTypeInfo = null;
        try {
          structTypeInfo = new ParquetToHiveSchemaConverter(tbl).convert(
              ParquetSchemaReader.read(parquetFile));
        } catch (IOException ioe) {
          LOG.error(ioe.getMessage(), ioe);
        } catch (UnsupportedOperationException ue) {
          LOG.error(ue.getMessage(), ue);
        } catch (RuntimeException ex) {
          LOG.error(ex.getMessage(), ex);
        }
        if (structTypeInfo == null) {
          columnNames = new ArrayList<String>();
          columnTypes = new ArrayList<TypeInfo>();
        } else {
          columnNames = structTypeInfo.getAllStructFieldNames();
          columnTypes = structTypeInfo.getAllStructFieldTypeInfos();
        }
      }
    } else {
      if (columnNameProperty.length() == 0) {
        columnNames = new ArrayList<String>();
      } else {
        columnNames = Arrays.asList(columnNameProperty.split(","));
      }
      if (columnTypeProperty.length() == 0) {
        columnTypes = new ArrayList<TypeInfo>();
      } else {
        columnTypes = TypeInfoUtils.getTypeInfosFromTypeString(columnTypeProperty);
      }
    }

    if (columnNames.size() != columnTypes.size()) {
      LOG.error("ParquetHiveSerde initialization failed. Number of column name and column type " +
          "differs. columnNames = " + columnNames + ", columnTypes = " + columnTypes);
      columnNames = new ArrayList<String>();
      columnTypes = new ArrayList<TypeInfo>();
    }
    // Create row related objects
    rowTypeInfo = TypeInfoFactory.getStructTypeInfo(columnNames, columnTypes);
    this.objInspector = new ArrayWritableObjectInspector((StructTypeInfo) rowTypeInfo);

    // Stats part
    serializedSize = 0;
    deserializedSize = 0;
    status = LAST_OPERATION.UNKNOWN;
  }

  @Override
  public Object deserialize(final Writable blob) throws SerDeException {
    status = LAST_OPERATION.DESERIALIZE;
    deserializedSize = 0;
    if (blob instanceof ArrayWritable) {
      deserializedSize = ((ArrayWritable) blob).get().length;
      return blob;
    } else {
      return null;
    }
  }

  @Override
  public ObjectInspector getObjectInspector() throws SerDeException {
    return objInspector;
  }

  @Override
  public Class<? extends Writable> getSerializedClass() {
    return ParquetHiveRecord.class;
  }

  @Override
  public Writable serialize(final Object obj, final ObjectInspector objInspector)
      throws SerDeException {
    if (!objInspector.getCategory().equals(Category.STRUCT)) {
      throw new SerDeException("Cannot serialize " + objInspector.getCategory() + ". Can only serialize a struct");
    }
    serializedSize = ((StructObjectInspector)objInspector).getAllStructFieldRefs().size();
    status = LAST_OPERATION.SERIALIZE;
    parquetRow.value = obj;
    parquetRow.inspector= (StructObjectInspector)objInspector;
    return parquetRow;
  }

  @Override
  public SerDeStats getSerDeStats() {
    // must be different
    assert (status != LAST_OPERATION.UNKNOWN);
    if (status == LAST_OPERATION.SERIALIZE) {
      stats.setRawDataSize(serializedSize);
    } else {
      stats.setRawDataSize(deserializedSize);
    }
    return stats;
  }

  /**
   * @param table
   * @return true if the table has the parquet serde defined
   */
  public static boolean isParquetTable(Table table) {
    return  table == null ? false : ParquetHiveSerDe.class.getName().equals(table.getSerializationLib());
  }

  private Path getParquetFile(Configuration conf, Path loc) {
    if (loc == null) {
      return null;
    }

    Path parquetFile;
    try {
      parquetFile = getAFile(FileSystem.get(new URI(loc.toString()), conf), loc);
    } catch (Exception e) {
      LOG.error("Unable to read file from " + loc + ": " + e, e);
      parquetFile = null;
    }

    return parquetFile;
  }

  private Path getAFile(FileSystem fs, Path path) throws IOException {
    FileStatus status = fs.getFileStatus(path);

    if (status.isFile()) {
      if (status.getLen() > 0) {
        return path;
      } else {
        return null;
      }
    }

    for(FileStatus childStatus: fs.listStatus(path)) {
      Path file = getAFile(fs, childStatus.getPath());

      if (file != null) {
        return file;
      }
    }

    return null;
  }
}
