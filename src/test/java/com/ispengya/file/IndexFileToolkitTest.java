package com.ispengya.file;

import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.index.FixedUnitIndexFile;
import com.ispengya.file.index.IndexCursorFile;
import com.ispengya.file.index.IndexEntryCodec;
import com.ispengya.file.index.RecordIndex;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * 通用索引文件工具包契约:
 *
 * <ul>
 *   <li>{@link FixedUnitIndexFile}:自定义定长 entry 的滚动/恢复/读取/截断;</li>
 *   <li>{@link IndexCursorFile}:原子游标持久化与损坏退化;</li>
 *   <li>两者组合成一个自定义 {@link RecordIndex},由 IndexHost 驱动。</li>
 * </ul>
 */
public class IndexFileToolkitTest {

    private static final String ROOT = "./target/index-file-toolkit-test";
    private static final int INDEX_FILE_SIZE = 48;
    private static final LogRecordCodec LOG_CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void fixedUnitIndexFileSupportsRollAppendRecoverAndTruncate() throws Exception {
        File dir = dirFor("fixed");
        FixedUnitIndexFile<Entry> index = FixedUnitIndexFile.open(dir.getPath(), INDEX_FILE_SIZE, ENTRY_CODEC);
        for (long i = 1; i <= 7; i++) {
            index.append(new Entry(i, i * 10));
        }
        assertEquals(7, index.count());
        assertEquals(1, index.get(0).id);
        assertEquals(70, index.get(6).payload);
        assertNull(index.get(7));
        assertEquals("48B 文件应滚成 3 个 16B entry/文件", 3, numericFiles(dir).length);

        index.flush();
        index.close();

        // 恢复:有效前缀扫描 + 从尾部继续追加
        FixedUnitIndexFile<Entry> reopened = FixedUnitIndexFile.open(dir.getPath(), INDEX_FILE_SIZE, ENTRY_CODEC);
        assertEquals(7, reopened.count());
        assertEquals(7, reopened.get(6).id);
        reopened.append(new Entry(8, 80));
        assertEquals(8, reopened.count());
        assertEquals(8, reopened.get(7).id);

        // 截断到 5 条:截断必须落盘,重启后不能复活旧 entry
        reopened.truncateTo(5);
        assertEquals(5, reopened.count());
        assertNull(reopened.get(5));
        reopened.close();

        FixedUnitIndexFile<Entry> afterTruncate = FixedUnitIndexFile.open(
                dir.getPath(), INDEX_FILE_SIZE, ENTRY_CODEC);
        assertEquals("截断结果必须在重开后保持", 5, afterTruncate.count());
        assertNull(afterTruncate.get(5));
        afterTruncate.append(new Entry(9, 90));
        assertEquals(6, afterTruncate.count());
        assertEquals(9, afterTruncate.get(5).id);
        afterTruncate.close();

        FixedUnitIndexFile<Entry> again = FixedUnitIndexFile.open(dir.getPath(), INDEX_FILE_SIZE, ENTRY_CODEC);
        assertEquals(6, again.count());
        assertEquals(9, again.get(5).id);
        again.close();
    }

    @Test
    public void indexCursorFilePersistsAtomicallyAndDegradesToFullReplay() throws Exception {
        File dir = dirFor("cursor");
        IndexCursorFile cursor = new IndexCursorFile(new File(dir, "cursor"));
        assertEquals(-1L, cursor.load());

        cursor.save(123L);
        assertEquals(123L, cursor.load());
        cursor.save(456L);
        assertEquals(456L, cursor.load());

        Files.write(cursor.getFile().toPath(), "not-a-number".getBytes(StandardCharsets.UTF_8));
        assertEquals("脏游标必须退化为全量回放", -1L, cursor.load());

        cursor.clear();
        assertEquals(-1L, cursor.load());
    }

    @Test
    public void fixedUnitFileAndCursorCanBackACustomRecordIndex() throws Exception {
        File dir = dirFor("plugin");
        StoreConfig config = StoreConfig.builder(dir.getPath())
                .commitLogFileSize(256)
                .maxBodySize(64)
                .build();

        RangeIndex first = new RangeIndex(new File(dir, "range-index"));
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, LOG_CODEC)
                .withIndex(first)
                .build()) {
            for (int i = 0; i < 5; i++) {
                store.put(record("r-" + i), 100L + i);
            }
            assertEquals(5, first.count());
            assertEquals(100L, first.entry(0).payload);
            assertEquals(104L, first.entry(4).payload);
        }

        // 重启:FixedUnitIndexFile 恢复已有 entry,cursor 避免重复回放
        RangeIndex second = new RangeIndex(new File(dir, "range-index"));
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, LOG_CODEC)
                .withIndex(second)
                .build()) {
            assertEquals(5, second.count());
            store.put(record("r-5"), 105L);
            assertEquals(6, second.count());
            assertEquals(105L, second.entry(5).payload);
        }

        RangeIndex third = new RangeIndex(new File(dir, "range-index"));
        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, LOG_CODEC)
                .withIndex(third)
                .build()) {
            assertEquals(6, third.count());
        }
    }

    /**
     * 自定义 CQ/IndexFile 的最小 entry:16B [id][payload]
     */
    static final class Entry {
        final long id;
        final long payload;

        Entry(long id, long payload) {
            this.id = id;
            this.payload = payload;
        }
    }

    /**
     * id > 0 作为有效性判据,全零预分配尾会自然终止恢复
     */
    static final IndexEntryCodec<Entry> ENTRY_CODEC = new IndexEntryCodec<Entry>() {
        @Override
        public int unitSize() {
            return 16;
        }

        @Override
        public Entry decode(ByteBuffer source) {
            return new Entry(source.getLong(), source.getLong());
        }

        @Override
        public void encode(Entry entry, ByteBuffer target) {
            target.putLong(entry.id);
            target.putLong(entry.payload);
        }

        @Override
        public boolean isValid(Entry entry) {
            return entry != null && entry.id > 0;
        }
    };

    /**
     * 用通用工具包实现的"自定义 CQ":每条 entry 记录日志 offset+1 与 tagCode
     */
    static final class RangeIndex implements RecordIndex {

        private final FixedUnitIndexFile<Entry> file;
        private final IndexCursorFile cursorFile;
        private volatile long cursor;

        RangeIndex(File dir) throws IOException {
            this.file = FixedUnitIndexFile.open(dir.getPath(), INDEX_FILE_SIZE, ENTRY_CODEC);
            this.cursorFile = new IndexCursorFile(new File(dir, "cursor"));
        }

        @Override
        public String name() {
            return "range-index";
        }

        @Override
        public long replayFrom(long logMaxOffset) {
            long stored = cursorFile.load();
            long from = stored >= 0 && stored <= logMaxOffset && file.count() > 0 ? stored : 0L;
            this.cursor = from;
            return from;
        }

        @Override
        public void onFrame(long physicalOffset, RecordFrame.Frame frame, byte[] body) {
            // offset+1 保证 id > 0;payload 存 tagCode
            file.append(new Entry(physicalOffset + 1, frame.getTagCode()));
            this.cursor = physicalOffset + frame.getTotalLen();
        }

        @Override
        public long progress() {
            return cursor;
        }

        @Override
        public void flush() {
            file.flush();
            cursorFile.save(cursor);
        }

        @Override
        public void close() {
            try {
                flush();
            } finally {
                file.close();
            }
        }

        long count() {
            return file.count();
        }

        Entry entry(long logicalIndex) {
            return file.get(logicalIndex);
        }
    }

    private static LogRecord record(String message) {
        return new LogRecord(1000L, "INFO", message);
    }

    private static File[] numericFiles(File dir) {
        File[] files = dir.listFiles((d, name) -> name.matches("\\d+"));
        return files == null ? new File[0] : files;
    }

    private static File dirFor(String name) {
        File dir = new File(ROOT, name);
        deleteRecursively(dir);
        if (!dir.mkdirs() && !dir.exists()) {
            throw new IllegalStateException("cannot create " + dir);
        }
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
