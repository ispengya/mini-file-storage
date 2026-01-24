package com.ispengya.file;

import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.MmapSequentialLog;
import com.ispengya.file.core.SequentialLog;
import com.ispengya.file.core.SequentialLogConfig;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.index.SimpleConsumeQueue;
import com.ispengya.file.model.LogRecord;
import com.ispengya.file.store.RecordStore;
import junit.framework.Assert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.time.Instant;
import java.util.List;


public class FileDemoTest {
    private SequentialLog log;
    private SimpleConsumeQueue consumeQueue;
    private KeyIndexFile keyIndexFile;
    private RecordStore<LogRecord> store;
    private RecordStore.StoreCheckpoint checkpoint;

    @Before
    public void setUp() throws Exception {
//        clearDir("./docs/file-demo-data-test");
//        clearDir("./docs/file-demo-cq-test");
//        clearDir("./docs/file-demo-key-index-test");
//        clearDir("./docs/file-demo-checkpoint-test");
        SequentialLogConfig config = new SequentialLogConfig("./docs/file-demo-data-test", 1024 * 1024 * 8, true);
        consumeQueue = new SimpleConsumeQueue("./docs/file-demo-cq-test", 1024 * 1024, true);
        keyIndexFile = new KeyIndexFile("./docs/file-demo-key-index-test", 1024 * 1024);
        log = new MmapSequentialLog(config);
        checkpoint = new RecordStore.StoreCheckpoint("./docs/file-demo-checkpoint-test/checkpoint.dat");
        store = new RecordStore<>(log, new LogRecordCodec(), consumeQueue, checkpoint);
    }

    @After
    public void tearDown() {
        store.close();
        keyIndexFile.close();
    }

    @Test
    public void testAppendAndReadByPhysicalOffset() {
        long[] offsets = new long[3];
        LogRecord[] records = new LogRecord[offsets.length];
        for (int i = 0; i < offsets.length; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "p-" + i);
            records[i] = record;
            offsets[i] = store.append(record, ts);
            System.out.println(record);
        }
        System.out.println("===========================");
        store.flush();
        for (int i = 0; i < offsets.length; i++) {
            LogRecord loaded = store.read(offsets[i]);
            System.out.println(loaded);
            Assert.assertEquals(records[i].getTimestamp(), loaded.getTimestamp());
            Assert.assertEquals(records[i].getLevel(), loaded.getLevel());
            Assert.assertEquals(records[i].getMessage(), loaded.getMessage());
        }
    }

    @Test
    public void testAppendAndReadByPhysicalOffsetMAX() {
        for (int i = 1; i <= 140000; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "max-" + i);
            store.append(record, ts);
        }
        store.flush();
    }

    @Test
    public void testPrintAllMessagesInLogMAX() {
        LogRecord logRecord = store.readByLogicalOffset(140000);
        System.out.println(logRecord);
    }

    @Test
    public void testReadByLogicalOffset() {
        int count = 4;
        for (int i = 0; i < count; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "logical-" + i);
            store.append(record, ts);
        }
        store.flush();
        for (int i = 0; i < count; i++) {
            LogRecord loaded = store.readByLogicalOffset(i);
            Assert.assertNotNull(loaded);
            Assert.assertEquals("logical-" + i, loaded.getMessage());
        }
    }

    @Test
    public void testQueryByTimeRange() {
        long base = Instant.now().toEpochMilli();
        for (int i = 0; i < 5; i++) {
            long ts = base + i * 1000;
            LogRecord record = new LogRecord(ts, "INFO", "time-" + i);
            store.append(record, ts);
            System.out.println(record);
        }
        store.flush();
        long begin = base + 1000;
        long end = base + 3000;
        List<LogRecord> result = store.queryByTimeRange(begin, end);
        System.out.println(result);
        Assert.assertEquals(3, result.size());
        Assert.assertEquals("time-1", result.get(0).getMessage());
        Assert.assertEquals("time-2", result.get(1).getMessage());
        Assert.assertEquals("time-3", result.get(2).getMessage());
    }

    @Test
    public void testKeyIndexQuery() {
        long[] offsets = new long[5];
        for (int i = 0; i < offsets.length; i++) {
            long ts = Instant.now().toEpochMilli();
            String msg = "hello-" + i;
            LogRecord record = new LogRecord(ts, "INFO", msg);
            long offset = store.append(record, ts);
            offsets[i] = offset;
            long keyHash = msg.hashCode() & 0xffffffffL;
            keyIndexFile.put(keyHash, offset);
            System.out.println(record);
        }
        store.flush();
        keyIndexFile.flush();
        String key = "hello-3";
        long keyHash = key.hashCode() & 0xffffffffL;
        List<Long> keyOffsets = keyIndexFile.get(keyHash);
        Assert.assertFalse(keyOffsets.isEmpty());
        boolean found = false;
        for (Long offset : keyOffsets) {
            LogRecord record = store.read(offset);
            if (key.equals(record.getMessage())) {
                found = true;
                break;
            }
        }
        Assert.assertTrue(found);
    }

    @Test
    public void testCheckpointWrittenOnFlush() {
        long ts = Instant.now().toEpochMilli();
        LogRecord record = new LogRecord(ts, "INFO", "checkpoint-test");
        long offset = store.append(record, ts);
        store.flush();
        long[] values = checkpoint.load();
        Assert.assertNotNull(values);
        Assert.assertEquals(log.getMaxOffset(), values[0]);
        Assert.assertTrue(values[1] >= 0);
        LogRecord loaded = store.read(offset);
        Assert.assertEquals(record.getMessage(), loaded.getMessage());
    }

    @Test
    public void testRecoverCommitLogAfterRestart() throws Exception {
        String baseDir = "./docs/file-demo-recover-data-test";
        SequentialLogConfig config1 = new SequentialLogConfig(baseDir, 1024 * 1024, true);
        SequentialLog log1 = new MmapSequentialLog(config1);
        SimpleConsumeQueue cq1 = new SimpleConsumeQueue(baseDir + "-cq", 1024 * 1024);
        RecordStore<LogRecord> store1 = new RecordStore<>(log1, new LogRecordCodec(), cq1);
        long[] offsets1 = new long[3];
        for (int i = 0; i < offsets1.length; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "recover-1-" + i);
            offsets1[i] = store1.append(record, ts);
        }
        store1.flush();
        store1.close();
        cq1.close();
        SequentialLogConfig config2 = new SequentialLogConfig(baseDir, 1024 * 1024, true);
        SequentialLog log2 = new MmapSequentialLog(config2);
        SimpleConsumeQueue cq2 = new SimpleConsumeQueue(baseDir + "-cq", 1024 * 1024);
        RecordStore<LogRecord> store2 = new RecordStore<>(log2, new LogRecordCodec(), cq2);
        long[] offsets2 = new long[2];
        for (int i = 0; i < offsets2.length; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "recover-2-" + i);
            offsets2[i] = store2.append(record, ts);
        }
        store2.flush();
        for (int i = 0; i < offsets1.length; i++) {
            LogRecord loaded = store2.read(offsets1[i]);
            System.out.println(loaded);
            Assert.assertEquals("recover-1-" + i, loaded.getMessage());
        }
        for (int i = 0; i < offsets2.length; i++) {
            LogRecord loaded = store2.read(offsets2[i]);
            System.out.println(loaded);
            Assert.assertEquals("recover-2-" + i, loaded.getMessage());
        }
        store2.close();
        cq2.close();
    }

    @Test
    public void testRecoverConsumeQueueAfterRestart() throws Exception {
        String dataDir = "./docs/file-demo-recover-cq-data-test";
        String cqDir = "./docs/file-demo-recover-cq-test";
        SequentialLogConfig config1 = new SequentialLogConfig(dataDir, 1024 * 1024, true);
        SequentialLog log1 = new MmapSequentialLog(config1);
        SimpleConsumeQueue cq1 = new SimpleConsumeQueue(cqDir, 1024 * 1024);
        RecordStore<LogRecord> store1 = new RecordStore<>(log1, new LogRecordCodec(), cq1);
        int firstCount = 3;
        for (int i = 0; i < firstCount; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "cq-1-" + i);
            store1.append(record, ts);
        }
        store1.flush();
        store1.close();
        cq1.close();
        SequentialLogConfig config2 = new SequentialLogConfig(dataDir, 1024 * 1024, true);
        SequentialLog log2 = new MmapSequentialLog(config2);
        SimpleConsumeQueue cq2 = new SimpleConsumeQueue(cqDir, 1024 * 1024, true);
        RecordStore<LogRecord> store2 = new RecordStore<>(log2, new LogRecordCodec(), cq2);
        int secondCount = 2;
        for (int i = 0; i < secondCount; i++) {
            long ts = Instant.now().toEpochMilli();
            LogRecord record = new LogRecord(ts, "INFO", "cq-2-" + i);
            store2.append(record, ts);
        }
        store2.flush();
        for (int i = 0; i < firstCount; i++) {
            LogRecord loaded = store2.readByLogicalOffset(i);
            System.out.println(loaded);
            Assert.assertEquals("cq-1-" + i, loaded.getMessage());
        }
        for (int i = 0; i < secondCount; i++) {
            LogRecord loaded = store2.readByLogicalOffset(firstCount + i);
            System.out.println(loaded);
            Assert.assertEquals("cq-2-" + i, loaded.getMessage());
        }
        store2.close();
        cq2.close();
    }

    private void clearDir(String path) {
        File dir = new File(path);
        if (!dir.exists()) {
            return;
        }
        deleteRecursively(dir);
    }

    private void deleteRecursively(File file) {
        if (file.isDirectory()) {
            File[] children = file.listFiles();
            if (children != null) {
                for (File child : children) {
                    deleteRecursively(child);
                }
            }
        }
        file.delete();
    }
}
