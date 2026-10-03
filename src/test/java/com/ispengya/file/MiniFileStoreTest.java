package com.ispengya.file;

import com.ispengya.file.api.FileStore;
import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 门面 API(FileStore / MiniFileStore)行为测试:
 * 物理/逻辑读取、key 查询、tag 范围查询、重启恢复、关闭幂等。
 */
public class MiniFileStoreTest {

    private static final String TEST_ROOT = "./target/mini-file-store-test";

    @Before
    public void setUp() throws Exception {
        // 尽力清理上一轮 JVM 留下的目录(进程退出后 mmap 锁已释放才能删掉);
        // 本轮各测试使用互不相同的子目录,避免跨用例污染。
        deleteRecursively(new File(TEST_ROOT));
    }

    /**
     * 每个用例独立目录:创建前先清掉同名残留
     */
    private File dirFor(String caseName) {
        File dir = new File(TEST_ROOT, caseName);
        deleteRecursively(dir);
        dir.deleteOnExit();
        return dir;
    }

    private FileStore<LogRecord> openStore(File dir) {
        return new MiniFileStore<>(StoreConfig.builder(dir.getPath()).build(), new LogRecordCodec());
    }

    private static LogRecord record(long ts, String message) {
        return new LogRecord(ts, "INFO", message);
    }

    @Test
    public void putAndGetByPhysicalOffset() {
        FileStore<LogRecord> store = openStore(dirFor("physical"));
        try {
            long offset = store.put(record(1000L, "hello-physical"));
            LogRecord loaded = store.get(offset);
            assertNotNull(loaded);
            assertEquals("hello-physical", loaded.getMessage());
            assertEquals(1000L, loaded.getTimestamp());
        } finally {
            store.close();
        }
    }

    @Test
    public void getByLogicalIndex() {
        FileStore<LogRecord> store = openStore(dirFor("logical-index"));
        try {
            for (int i = 0; i < 5; i++) {
                store.put(record(2000L + i, "msg-" + i), 2000L + i);
            }
            for (int i = 0; i < 5; i++) {
                LogRecord loaded = store.getByLogicalIndex(i);
                assertNotNull("logical index " + i + " should exist", loaded);
                assertEquals("msg-" + i, loaded.getMessage());
            }
            // 越界位点返回 null
            assertNull(store.getByLogicalIndex(5));
        } finally {
            store.close();
        }
    }

    @Test
    public void putWithKeyAndQueryByKey() {
        FileStore<LogRecord> store = openStore(dirFor("key-query"));
        try {
            store.put(record(3000L, "order-a"), "order-1");
            store.put(record(3001L, "order-b"), "order-1");
            store.put(record(3002L, "user-x"), "user-9");

            List<LogRecord> orders = store.getByKey("order-1");
            assertEquals(2, orders.size());
            assertEquals("order-a", orders.get(0).getMessage());
            assertEquals("order-b", orders.get(1).getMessage());

            assertTrue(store.getByKey("not-exist").isEmpty());
        } finally {
            store.close();
        }
    }

    @Test
    public void queryByTagRange() {
        FileStore<LogRecord> store = openStore(dirFor("tag-range"));
        try {
            store.put(record(100L, "too-early"), 100L);
            store.put(record(200L, "in-range-1"), 200L);
            store.put(record(250L, "in-range-2"), 250L);
            store.put(record(400L, "too-late"), 400L);

            List<LogRecord> range = store.queryByTagRange(150L, 300L);
            assertEquals(2, range.size());
            assertEquals("in-range-1", range.get(0).getMessage());
            assertEquals("in-range-2", range.get(1).getMessage());
        } finally {
            store.close();
        }
    }

    @Test
    public void defaultPutTagIsWallClockTime() {
        FileStore<LogRecord> store = openStore(dirFor("default-tag"));
        try {
            long now = System.currentTimeMillis();
            store.put(record(now, "default-tag"));

            List<LogRecord> recent = store.queryByTagRange(now - 1000L, now + 60_000L);
            assertEquals(1, recent.size());
            assertEquals("default-tag", recent.get(0).getMessage());
        } finally {
            store.close();
        }
    }

    @Test
    public void restartRecoveryKeepsAllIndexTypes() {
        File dir = dirFor("restart");
        FileStore<LogRecord> store = openStore(dir);
        store.put(record(5000L, "before-1"), 5000L);
        store.put(record(5001L, "before-2"), 5001L);
        store.put(record(5002L, "keyed-before"), "k-1");
        store.flush();
        store.close();

        // 重新打开:CommitLog / ConsumeQueue / KeyIndex 均应能继续追加并读到旧数据
        FileStore<LogRecord> reopened = openStore(dir);
        try {
            assertEquals("before-1", reopened.getByLogicalIndex(0).getMessage());
            assertEquals("before-2", reopened.getByLogicalIndex(1).getMessage());
            assertEquals("keyed-before", reopened.getByKey("k-1").get(0).getMessage());

            long newOffset = reopened.put(record(5003L, "after-restart"), "k-2");
            assertEquals("after-restart", reopened.get(newOffset).getMessage());
            assertEquals("after-restart", reopened.getByLogicalIndex(3).getMessage());
            assertEquals("after-restart", reopened.getByKey("k-2").get(0).getMessage());
        } finally {
            reopened.close();
        }
    }

    @Test
    public void closeIsIdempotent() {
        FileStore<LogRecord> store = openStore(dirFor("close-twice"));
        store.put(record(6000L, "twice-close"));
        store.close();
        store.close();
    }

    private static void deleteRecursively(File file) {
        if (!file.exists()) {
            return;
        }
        try {
            Files.walk(file.toPath())
                    .sorted(Comparator.reverseOrder())
                    .forEach(path -> {
                        try {
                            Files.delete(path);
                        } catch (Exception e) {
                            // Windows 下 mmap 未释放时可能删除失败;各用例目录互不相同,不影响本轮结果
                        }
                    });
        } catch (Exception ignored) {
        }
    }
}
