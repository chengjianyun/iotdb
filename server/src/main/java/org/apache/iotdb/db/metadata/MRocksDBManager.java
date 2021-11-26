package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.utils.SchemaUtils;

import org.rocksdb.Holder;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class MRocksDBManager {

  private static final String dbPath = "./rocksdb-data/";
  private static final String cfdbPath = "./rocksdb-data-cf/";

  private final Options options;
  private final RocksDB rocksDB;

  private static final String ROOT = "r";
  private static final String SG_NODE_PREFIX = "G";
  private static final String MEASUREMENT_NODE_PREFIX = "M";

  static {
    RocksDB.loadLibrary();
  }

  public MRocksDBManager() throws RocksDBException {
    options = new Options();
    options.setCreateIfMissing(true);

    rocksDB = RocksDB.open(options, dbPath);
  }

  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    String[] nodes = storageGroup.getNodes();
    try {
      createStorageGroup(nodes, nodes.length, nodes.length);
    } catch (RocksDBException e) {
      e.printStackTrace();
    }
  }

  /**
   * For given nodes in order, create needed node recursively. e.g. Input nodes: [root, a, b, c],
   * Then below nodes will be created recursively if non-exist:
   *
   * <p>r.Ga.Gb.Gc -> [attributes]
   *
   * <p>r.a.b -> [c, ...]
   *
   * <p>r.a -> [b, ...]
   *
   * <p>r -> [a, ...]
   *
   * @param nodes of sg path
   * @param i
   * @param length of nodes
   * @throws RocksDBException, throw Rocksdb exception if natively exception happened
   */
  private void createStorageGroup(String[] nodes, int i, int length) throws RocksDBException {
    if (length <= 1) {
      return;
    }
    String key;
    if (i == length) {
      key = createKey(nodes, i, SG_NODE_PREFIX);
    } else {
      key = createKey(nodes, i);
    }
    Holder<byte[]> holder = new Holder<>();
    if (rocksDB.keyMayExist(key.getBytes(), holder)) {
      if (holder.getValue() != null) {
        if (i < length) {
          rocksDB.merge(key.getBytes(), nodes[i].getBytes());
        }
        return;
      }
    }
    createStorageGroup(nodes, i - 1, length);
  }

  private String createKey(String[] nodes, int len, String prefix) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT).append(PATH_SEPARATOR).append(prefix);
    for (int i = 1; i < len; i++) {
      builder.append(nodes[i]).append(PATH_SEPARATOR).append(prefix);
    }
    builder.append(len - 1);
    return builder.toString();
  }

  private String createKey(String[] nodes, int length) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT).append(PATH_SEPARATOR);
    for (int i = 1; i < length - 1; i++) {
      builder.append(nodes[i]).append(PATH_SEPARATOR).append(SG_NODE_PREFIX);
    }
    builder.append(length - 1);
    return builder.toString();
  }

  private String createInternalKey(String[] nodes, int length) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT).append(PATH_SEPARATOR);
    for (int i = 1; i < length - 1; i++) {
      builder.append(nodes[i]).append(PATH_SEPARATOR);
    }
    builder.append(length - 1);
    return builder.toString();
  }

  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    PartialPath path = plan.getPath();
    SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

    // check storage group, create if not exist and auto create schema enabled, otherwise throw
    // StorageGroupNoTSet exception
    // TODO: check storage group

    
  }
}
