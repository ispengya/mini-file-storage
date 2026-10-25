package com.ispengya.file;

import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.index.RecordIndex;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 自定义索引插件契约:实现 {@link RecordIndex} 即可由内核在启动时全量回放、
 * 写入时增量驱动;新增索引不会重复回放已有索引。
 */
public class CustomIndexTest {

    private static final String ROOT = "./target/custom-index-test";
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void customMemoryIndexBuildsOnOpenAndTracksWrites() {
        File dir = dirFor("memory");
        StoreConfig config = config(dir);
        MemoryKvIndex memory = new MemoryKvIndex();

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withIndex(memory)
                .build()) {
            store.put(record("a"), 1L);
            store.put(record("b"), 2L);

            // SYNC 模式 put 返回时自定义索引已收到帧
            assertEquals("a", memory.get("a").getMessage());
            assertEquals("b", memory.get("b").getMessage());
            assertSame(memory, store.index(KvView.class).orElseThrow());
            assertFalse(store.logical().isPresent());
            assertFalse(store.keys().isPresent());
        }

        assertFalse("自定义索引不应创建内置索引目录",
                new File(config.consumeQueuePath()).exists());
        assertFalse(new File(config.keyIndexPath()).exists());

        // 重启:replayFrom()=0 → 从 CommitLog 全量重建
        MemoryKvIndex rebuilt = new MemoryKvIndex();
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withIndex(rebuilt)
                .build()) {
            assertEquals(2, rebuilt.size());
            assertEquals("b", rebuilt.get("b").getMessage());
            store.put(record("c"), 3L);
            assertEquals("c", rebuilt.get("c").getMessage());
        }
    }

    @Test
    public void addingCustomIndexLaterReplaysOnlyIt() {
        File dir = dirFor("add-later");
        StoreConfig config = config(dir);

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withConsumeQueue()
                .build()) {
            store.put(record("a"), 10L);
            store.put(record("b"), 11L);
            store.put(record("c"), 12L);
        }

        MemoryKvIndex added = new MemoryKvIndex();
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC)
                .withConsumeQueue()
                .withIndex(added)
                .build()) {
            // 新索引从 0 全量回放;已有 CQ 从自身 cursor 续传,不重复写入
            assertEquals(3, added.size());
            assertEquals(3L, store.logical().orElseThrow().logicalCount());
            assertEquals("a", store.getByLogicalIndex(0).getMessage());

            store.put(record("d"), 13L);
            assertEquals("d", added.get("d").getMessage());
            assertEquals("d", store.getByLogicalIndex(3).getMessage());
        }
    }

    @Test
    public void duplicateIndexNamesAreRejectedBeforeOpeningResources() {
        StoreConfig config = config(dirFor("duplicate"));
        try {
            MiniFileStore.builder(config, CODEC)
                    .withIndex(new MemoryKvIndex())
                    .withIndex(new MemoryKvIndex())
                    .build();
            fail("重复索引名必须拒绝");
        } catch (IllegalArgumentException e) {
            assertTrue(e.getMessage().contains("duplicate record index name"));
        }
    }

    /**
     * 测试用视图接口:验证 store.index(Class) 能找到自定义插件的能力
     */
    interface KvView {
        LogRecord get(String key);
    }

    /**
     * 测试用内存 KV 索引:key = 记录的 message,启动时全量重建
     */
    static final class MemoryKvIndex implements RecordIndex, KvView {

        private final Map<String, LogRecord> map = new HashMap<>();
        private final LogRecordCodec codec = new LogRecordCodec();
        private volatile long cursor;

        @Override
        public String name() {
            return "memory-kv";
        }

        @Override
        public long replayFrom(long logMaxOffset) {
            this.cursor = 0L;
            return 0L;
        }

        @Override
        public void onFrame(long physicalOffset, RecordFrame.Frame frame, byte[] body) {
            LogRecord record = codec.decode(body);
            map.put(record.getMessage(), record);
            this.cursor = physicalOffset + frame.getTotalLen();
        }

        @Override
        public long progress() {
            return cursor;
        }

        @Override
        public void flush() {
            // 内存派生视图无需落盘
        }

        @Override
        public LogRecord get(String key) {
            return map.get(key);
        }

        int size() {
            return map.size();
        }
    }

    private static StoreConfig config(File dir) {
        return StoreConfig.builder(dir.getPath()).build();
    }

    private static LogRecord record(String message) {
        return new LogRecord(1000L, "INFO", message);
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
