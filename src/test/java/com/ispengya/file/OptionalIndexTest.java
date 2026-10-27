package com.ispengya.file;

import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 可选索引装配契约:
 *
 * <ul>
 *   <li>builder 默认 == 纯 WAL:不建索引目录、不启索引线程、只有物理 offset 读写;</li>
 *   <li>只开 CQ:逻辑位点/tag 范围可用,key 查询不可用;</li>
 *   <li>只开 KeyIndex:key 查询可用,逻辑位点不可用;</li>
 *   <li>未启用的能力给出明确的"索引未启用"错误。</li>
 * </ul>
 */
public class OptionalIndexTest {

    private static final String ROOT = "./target/optional-index-test";
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void builderWithoutIndexesIsPureWal() {
        File dir = dirFor("pure-wal");
        StoreConfig config = config(dir);
        long offset;

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC).build()) {
            offset = store.put(record("wal-1"), 1L);
            assertEquals("wal-1", store.get(offset).getMessage());
            assertFalse(store.logical().isPresent());
            assertFalse(store.keys().isPresent());
            expectNotEnabled(() -> store.getByLogicalIndex(0), "consume queue");
            expectNotEnabled(() -> store.getByKey("k"), "key index");
            expectNotEnabled(() -> store.queryByTagRange(0L, 1L), "consume queue");
        }

        assertFalse("纯 WAL 不应创建 CQ 目录", new File(config.consumeQueuePath()).exists());
        assertFalse("纯 WAL 不应创建 KeyIndex 目录", new File(config.keyIndexPath()).exists());

        // 重启后仍可按物理 offset 读取
        try (MiniFileStore<LogRecord> reopened = MiniFileStore.builder(config, CODEC).build()) {
            assertEquals("wal-1", reopened.get(offset).getMessage());
            long next = reopened.put(record("wal-2"), 2L);
            assertEquals("wal-2", reopened.get(next).getMessage());
        }
    }

    @Test
    public void staleKeyEntriesBeyondRecoveredLogAreIgnored() throws Exception {
        File dir = dirFor("stale-key");
        StoreConfig config = config(dir);
        long[] offsets = new long[3];

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withKeyIndex()
                .build()) {
            offsets[0] = store.put(record("a"), "key-a");
            offsets[1] = store.put(record("b"), "key-b");
            offsets[2] = store.put(record("c"), "key-c");
        }

        // 模拟最后一帧所在页未落盘:抹掉 magic,全局恢复会在该帧前截断
        try (java.io.RandomAccessFile raf = new java.io.RandomAccessFile(
                new File(config.commitLogPath(), "00000000000000000000"), "rw")) {
            raf.seek(offsets[2]);
            raf.write(new byte[4]);
            raf.getChannel().force(true);
        }

        try (MiniFileStore<LogRecord> reopened = MiniFileStore.builder(config, CODEC)
                .withKeyIndex()
                .build()) {
            assertEquals("b", reopened.getByKey("key-b").get(0).getMessage());
            // KeyIndex 里可能残留截断后的旧条目;必须被过滤,而不是抛 offset out of range
            assertTrue("截断区之后的 key 不应可见", reopened.getByKey("key-c").isEmpty());
        }
    }

    @Test
    public void disabledIndexesResumeFromTheirOwnCursorWhenReEnabled() {
        File dir = dirFor("re-enable");
        StoreConfig config = config(dir);

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withDefaultIndexes()
                .build()) {
            store.put(record("a"), "key-a");
            store.put(record("b"), "key-b");
        }

        // 中间一段时间用纯 WAL 写入:索引不参与,但帧头仍保留 tag/keyHash
        try (MiniFileStore<LogRecord> wal = MiniFileStore.builder(config, CODEC).build()) {
            wal.put(record("c"), "key-c");
        }

        // 重新启用:CQ 从自时钟、KeyIndex 从 cursor 文件续传,补上纯 WAL 期间的那一帧
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withDefaultIndexes()
                .build()) {
            assertEquals(3L, store.logical().orElseThrow(IllegalStateException::new).logicalCount());
            assertEquals("c", store.getByLogicalIndex(2).getMessage());
            assertEquals("c", store.getByKey("key-c").get(0).getMessage());
        }
    }

    @Test
    public void builtInIndexesCanBeAddedToExistingWal() {
        File dir = dirFor("add-builtins");
        StoreConfig config = config(dir);

        // 先用纯 WAL 写入:帧头仍保留 keyHash/tagCode,只是当时没有建索引
        try (MiniFileStore<LogRecord> wal = MiniFileStore.builder(config, CODEC).build()) {
            wal.put(record("a"), "key-a");
            wal.put(record("b"), "key-b");
        }
        assertFalse(new File(config.consumeQueuePath()).exists());
        assertFalse(new File(config.keyIndexPath()).exists());

        // 后加内置索引:两个插件都从 0 全量回放,历史数据立即可查
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withDefaultIndexes()
                .build()) {
            assertEquals(2L, store.logical().orElseThrow(IllegalStateException::new).logicalCount());
            assertEquals("a", store.getByLogicalIndex(0).getMessage());
            assertEquals("b", store.getByKey("key-b").get(0).getMessage());
        }
    }

    @Test
    public void consumeQueueOnlyIndex() {
        File dir = dirFor("cq-only");
        StoreConfig config = config(dir);

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withConsumeQueue()
                .build()) {
            for (int i = 0; i < 3; i++) {
                store.put(record("cq-" + i), 100L + i);
            }
            assertEquals(3L, store.logical().orElseThrow(IllegalStateException::new).logicalCount());
            assertEquals("cq-1", store.getByLogicalIndex(1).getMessage());
            assertEquals(2, store.queryByTagRange(101L, 102L).size());
            assertFalse(store.keys().isPresent());
            expectNotEnabled(() -> store.getByKey("k"), "key index");
        }

        assertTrue(new File(config.consumeQueuePath()).exists());
        assertFalse("未启用 KeyIndex 不应创建目录", new File(config.keyIndexPath()).exists());

        try (MiniFileStore<LogRecord> reopened = MiniFileStore.builder(config, CODEC)
                .withConsumeQueue()
                .build()) {
            assertEquals("cq-2", reopened.getByLogicalIndex(2).getMessage());
            reopened.put(record("cq-3"), 103L);
            assertEquals("cq-3", reopened.getByLogicalIndex(3).getMessage());
        }
    }

    @Test
    public void keyIndexOnlyIndex() {
        File dir = dirFor("key-only");
        StoreConfig config = config(dir);

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withKeyIndex()
                .build()) {
            store.put(record("k-a"), "key-a");
            store.put(record("k-b"), "key-b");
            assertEquals("k-a", store.getByKey("key-a").get(0).getMessage());
            assertFalse(store.logical().isPresent());
            expectNotEnabled(() -> store.getByLogicalIndex(0), "consume queue");
        }

        assertTrue(new File(config.keyIndexPath()).exists());
        assertTrue("KeyIndex 需要独立游标文件",
                new File(config.keyIndexPath(), "cursor").exists());
        assertFalse("未启用 CQ 不应创建目录", new File(config.consumeQueuePath()).exists());

        // 重启:key 游标续传,旧数据与新写入都能查
        try (MiniFileStore<LogRecord> reopened = MiniFileStore.builder(config, CODEC)
                .withKeyIndex()
                .build()) {
            assertEquals("k-b", reopened.getByKey("key-b").get(0).getMessage());
            reopened.put(record("k-c"), "key-c");
            assertEquals("k-c", reopened.getByKey("key-c").get(0).getMessage());
        }
    }

    private static StoreConfig config(File dir) {
        return StoreConfig.builder(dir.getPath()).build();
    }

    private static LogRecord record(String message) {
        return new LogRecord(1000L, "INFO", message);
    }

    private static void expectNotEnabled(Runnable action, String fragment) {
        try {
            action.run();
            fail("未启用索引时调用对应能力必须失败");
        } catch (IllegalStateException e) {
            assertTrue("错误信息应包含 " + fragment + ", 实际: " + e.getMessage(),
                    e.getMessage().contains(fragment));
        }
    }

    private static File dirFor(String name) {
        File dir = new File(ROOT, name);
        deleteRecursively(dir);
        return dir;
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
