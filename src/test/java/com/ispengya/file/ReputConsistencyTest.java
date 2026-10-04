package com.ispengya.file;

import com.ispengya.file.api.FileStore;
import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.index.KeyIndexFile;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Reput 接线后的门面一致性行为:
 * ASYNC 显式 await 升级、close 收尾回放(stopAndDrain)、getByKey 读去重。
 *
 * <p>注:"ASYNC 下 put 后立刻读索引读到 null"这类断言刻意不写——后台线程够快时
 * 它可能不为真,属于计时器假设而非语义保证。语义保证只有两条:
 * 不 await 不保证可见(异步),await 之后必须可见(强)。本测试断言后者。</p>
 */
public class ReputConsistencyTest {

    private static final String ROOT = "./target/reput-consistency-test";
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void asyncAwaitUpgradesToStrongConsistency() {
        StoreConfig config = StoreConfig.builder(dir("async"))
                .indexMode(StoreConfig.IndexMode.ASYNC)
                .build();
        FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
        try {
            long[] offsets = new long[10];
            for (int i = 0; i < offsets.length; i++) {
                offsets[i] = store.put(new LogRecord(2000L + i, "INFO", "async-" + i), 2000L + i);
            }

            long before = store.maxIndexedOffset();
            assertTrue("await 必须在超时内追平最后一条",
                    store.awaitIndexed(offsets[offsets.length - 1], 5000));
            long after = store.maxIndexedOffset();
            assertTrue("游标单调不减", after >= before);
            assertTrue("覆盖谓词:严格越过最后一条的起始位点", after > offsets[offsets.length - 1]);

            for (int i = 0; i < offsets.length; i++) {
                assertEquals("async-" + i, store.getByLogicalIndex(i).getMessage());
            }

            // 等待一个永远不会被覆盖的位点:必须超时返回 false(而非阻塞/抛错)
            assertFalse(store.awaitIndexed(after + 10_000L, 60));
        } finally {
            store.close();
        }
    }

    @Test
    public void closeDrainsPendingIndexes() {
        StoreConfig config = StoreConfig.builder(dir("close-drain"))
                .indexMode(StoreConfig.IndexMode.ASYNC)
                .build();
        FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
        for (int i = 0; i < 5; i++) {
            LogRecord rec = new LogRecord(3000L + i, "INFO", "drain-" + i);
            if (i == 4) {
                store.put(rec, "last-key");
            } else {
                store.put(rec, 3000L + i);
            }
        }
        // 故意不 await 直接 close:stopAndDrain 必须把待建索引收尾再落盘
        store.close();

        FileStore<LogRecord> reopened = new MiniFileStore<>(
                StoreConfig.builder(dir("close-drain")).build(), CODEC);
        try {
            assertEquals("drain-4", reopened.getByLogicalIndex(4).getMessage());
            assertEquals(1, reopened.getByKey("last-key").size());
            assertEquals("drain-4", reopened.getByKey("last-key").get(0).getMessage());
        } finally {
            reopened.close();
        }
    }

    @Test
    public void getByKeyDedupsOverlappingEntries() throws Exception {
        String base = dir("dedup");
        StoreConfig config = StoreConfig.builder(base).build();
        long off0;
        long off1;
        FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
        try {
            off0 = store.put(new LogRecord(4000L, "INFO", "d-0"), "dup");
            off1 = store.put(new LogRecord(4001L, "INFO", "d-1"), "dup");
        } finally {
            store.close();
        }

        // 手工注入一条重复索引单元,模拟"崩溃在 key 写入与 cq 写入之间、回放重叠"的产物
        long dupHash = "dup".hashCode() & 0xffffffffL;
        KeyIndexFile manual = new KeyIndexFile(config.keyIndexPath(), 64 * 1024, true);
        try {
            manual.put(dupHash, off0);
            manual.flush();
        } finally {
            manual.close();
        }
        assertTrue(off0 < off1);

        FileStore<LogRecord> reopened = new MiniFileStore<>(StoreConfig.builder(base).build(), CODEC);
        try {
            // 三条索引单元 [off0, off1, off0] → 去重后两条记录,顺序保留
            assertEquals(2, reopened.getByKey("dup").size());
            assertEquals("d-0", reopened.getByKey("dup").get(0).getMessage());
            assertEquals("d-1", reopened.getByKey("dup").get(1).getMessage());
        } finally {
            reopened.close();
        }
    }

    private static String dir(String name) {
        return new File(ROOT, name).getPath();
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
                        } catch (Exception ignored) {
                        }
                    });
        } catch (Exception ignored) {
        }
    }
}
