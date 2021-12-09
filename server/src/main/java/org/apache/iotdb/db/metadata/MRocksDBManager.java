package org.apache.iotdb.db.metadata;

import org.apache.iotdb.db.exception.metadata.MetadataException;
import org.apache.iotdb.db.exception.metadata.PathAlreadyExistException;
import org.apache.iotdb.db.metadata.mnode.EntityMNode;
import org.apache.iotdb.db.metadata.mnode.IMNode;
import org.apache.iotdb.db.metadata.path.PartialPath;
import org.apache.iotdb.db.qp.physical.sys.CreateTimeSeriesPlan;
import org.apache.iotdb.db.qp.physical.sys.SetStorageGroupPlan;
import org.apache.iotdb.db.utils.SchemaUtils;
import org.apache.iotdb.tsfile.file.metadata.enums.CompressionType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSDataType;
import org.apache.iotdb.tsfile.file.metadata.enums.TSEncoding;

import com.google.common.util.concurrent.Striped;
import org.rocksdb.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.concurrent.locks.Lock;

import static org.apache.iotdb.db.metadata.MManager.*;
import static org.apache.iotdb.tsfile.common.constant.TsFileConstant.PATH_SEPARATOR;

public class MRocksDBManager {

  public static final String dbPath = "./rocksdb-data/";
  private static final String cfdbPath = "./rocksdb-data-cf/";

  private final Options options;
  private final RocksDB rocksDB;

  private static final String ROOT = "r";
  private static final String SG_NODE_PREFIX = "s";
  private static final String DEVICE_NODE_PREFIX = "d";
  private static final String MEASUREMENT_NODE_PREFIX = "m";

  private static final Logger logger = LoggerFactory.getLogger(MRocksDBManager.class);

  static {
    RocksDB.loadLibrary();
  }

  public MRocksDBManager() throws RocksDBException {
    options = new Options();
    options.setCreateIfMissing(true);

    rocksDB = RocksDB.open(options, dbPath);
    // TODO: make sure `root` existed
    if (rocksDB.get(ROOT.getBytes()) == null) {
      rocksDB.put(ROOT.getBytes(), "".getBytes());
    }
  }

  private void initRootNode() {
    Holder holder = new Holder();
    if (rocksDB.keyMayExist(ROOT.getBytes(), holder) && holder.getValue() != null) {
      return;
    }
  }

  public void setStorageGroup(PartialPath storageGroup) throws MetadataException {
    String[] nodes = storageGroup.getNodes();
    try {
      insertByNodes(nodes, nodes.length, nodes.length);
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  // TODO: check how Stripped Lock consume memory
  Striped<Lock> locks = Striped.lazyWeakLock(10000);

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
  private void insertByNodes(String[] nodes, int i, int length)
      throws RocksDBException, PathAlreadyExistException {
    if (i < 1) {
      return;
    }

    String key;
    key = createNormalKey(nodes, i);

    boolean exist = keyExist(key);
    if (exist && i >= length) {
      throw new PathAlreadyExistException(key);
    }

    if (!exist) {
      insertByNodes(nodes, i - 1, length);
      String value = "";
      if (i < length) {
        value = nodes[i];
      }
      createValue(key, value);
    } else {
      appendValue(key, nodes[i]);
    }
  }

  private void appendValue(String key, String toAppend) throws RocksDBException {
    Lock myLock = locks.get(key);
    while (true) {
      if (myLock.tryLock()) {
        try {
          byte[] origin = rocksDB.get(key.getBytes());
          String originStr = new String(origin);
          if (originStr.indexOf(toAppend) == -1) {
            byte[] appendVal = (toAppend + "." + originStr).getBytes();
            rocksDB.put(key.getBytes(), appendVal);
          }
        } finally {
          myLock.unlock();
        }
        break;
      }
    }
  }

  private void createValue(String key, String value) throws RocksDBException {
    Lock myLock = locks.get(key);
    while (true) {
      if (myLock.tryLock()) {
        try {
          byte[] childValue = value.getBytes();
          rocksDB.put(key.getBytes(), childValue);
        } finally {
          myLock.unlock();
        }
        break;
      }
    }
  }

  public void scanAllKeys() {
    RocksIterator iterator = rocksDB.newIterator();
    System.out.println("\n-----------------scan rocksdb start----------------------");
    iterator.seekToFirst();
    while (iterator.isValid()) {
      logger.info("{} -> {}", new String(iterator.key()), new String(iterator.value()));
      iterator.next();
    }
    System.out.println("\n-----------------scan rocksdb end----------------------");
  }

  public void createTimeseries(
      PartialPath path,
      TSDataType dataType,
      TSEncoding encoding,
      CompressionType compressor,
      Map<String, String> props,
      String alias)
      throws MetadataException {
    String[] nodes = path.getNodes();
    try {
      insertByNodes(nodes, nodes.length, nodes.length);
    } catch (RocksDBException e) {
      throw new MetadataException(e);
    }
  }

  public IMNode queryByPath(String path) throws RocksDBException {
    byte[] value = rocksDB.get(path.getBytes());
    IMNode node = new EntityMNode(null, path);
    return node;
  }

  private boolean keyExist(String key, Holder<byte[]> holder) {
    if (!rocksDB.keyMayExist(key.getBytes(), holder)) {
      return false;
    }

    if (holder.getValue() == null) {
      return false;
    }
    return true;
  }

  private boolean keyExist(String key) {
    return keyExist(key, new Holder<>());
  }

  private String createSgKey(String[] nodes) {
    return createKeyWithPrefix(nodes, SG_NODE_PREFIX);
  }

  private String createDeviceKey(String[] nodes) {
    return createKeyWithPrefix(nodes, DEVICE_NODE_PREFIX);
  }

  private String createMeasurementKey(String[] nodes) {
    return createKeyWithPrefix(nodes, MEASUREMENT_NODE_PREFIX);
  }

  private String createKeyWithPrefix(String[] nodes, String prefix) {
    StringBuilder builder = new StringBuilder();
    builder.append(prefix).append(PATH_SEPARATOR).append(ROOT);
    for (int i = 1; i < nodes.length; i++) {
      builder.append(PATH_SEPARATOR).append(nodes[i]);
    }
    return builder.toString();
  }

  private String createNormalKey(String[] nodes, int end) {
    StringBuilder builder = new StringBuilder();
    builder.append(ROOT);
    for (int i = 1; i < end; i++) {
      builder.append(PATH_SEPARATOR).append(nodes[i]);
    }
    return builder.toString();
  }

  public void benchStorageGroupCreation() {
    BenchmarkTask<SetStorageGroupPlan> task =
        new BenchmarkTask<>(storageGroups, STORAGE_GROUP_THREAD_NUM, "create storage group", 100);
    task.runWork(
        setStorageGroupPlan -> {
          try {
            setStorageGroup(setStorageGroupPlan.getPath());
            return true;
          } catch (Exception e) {
            return false;
          }
        });
  }

  public void benchTimeSeriesCreation() {
    BenchmarkTask<List<CreateTimeSeriesPlan>> task =
        new BenchmarkTask<>(timeSeriesSet, TIME_SERIES_THREAD_NUM, "create storage group", 100);
    task.runWork(
        createTimeSeriesPlans -> {
          createTimeSeriesPlans.stream()
              .forEach(
                  ts -> {
                    try {
                      createTimeseries(
                          ts.getPath(),
                          ts.getDataType(),
                          ts.getEncoding(),
                          ts.getCompressor(),
                          ts.getProps(),
                          ts.getAlias());
                    } catch (MetadataException e) {
                      e.printStackTrace();
                    }
                  });
          return true;
        });
  }

  public void benchQuery() {
    BenchmarkTask<String> task =
        new BenchmarkTask<>(queryTsSet, QUERY_THREAD_NUM, "create storage group", 100);
    task.runWork(
        s -> {
          try {
            IMNode node = queryByPath(s);
            if (node != null) {
              node.toString();
            }
            return true;
          } catch (Exception e) {
            return false;
          }
        });
  }

  public void createTimeseries(CreateTimeSeriesPlan plan, long offset) throws MetadataException {
    PartialPath path = plan.getPath();
    SchemaUtils.checkDataTypeWithEncoding(plan.getDataType(), plan.getEncoding());

    // check storage group, create if not exist and auto create schema enabled, otherwise throw
    // StorageGroupNoTSet exception
    // TODO: check storage group

  }
}
