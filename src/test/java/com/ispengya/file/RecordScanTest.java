package com.ispengya.file;

import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.RecordScanner;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 只读扫描 API 契约:固定快照、跨文件跳过浪费区、按 offset 续扫、解码访问器。
 */
public class RecordScanTest {

    private static final String ROOT = "./target/record-scan-test";
    private static final int FILE_SIZE = 256;
    private static final int MAX_BODY_SIZE = 64;
    private static final int RECORDS = 10;
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void scansAllRecordsAcrossFileRollover() {
        File dir = dirFor("all");
        StoreConfig config = config(dir);
        long[] offsets = new long[RECORDS];

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC).build()) {
            for (int i = 0; i < RECORDS; i++) {
                offsets[i] = store.put(record(message(i)), 1000L + i);
            }

            int index = 0;
            try (RecordScanner scanner = store.scan()) {
                while (scanner.next()) {
                    assertEquals("扫描顺序必须与写入一致", offsets[index], scanner.offset());
                    assertEquals(message(index), CODEC.decode(scanner.body()).getMessage());
                    assertEquals(1000L + index, scanner.frame().getTagCode());
                    assertTrue(scanner.frame().getTotalLen() > 0);
                    index++;
                }
                assertEquals(RECORDS, index);
                assertEquals("快照尾 = 最后一帧结束位置",
                        offsets[RECORDS - 1] + frameLen(), scanner.endOffset());
            }
        }
    }

    @Test
    public void scanFromOffsetContinuesAtFrameBoundary() {
        File dir = dirFor("resume");
        StoreConfig config = config(dir);
        long[] offsets = new long[RECORDS];

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC).build()) {
            for (int i = 0; i < RECORDS; i++) {
                offsets[i] = store.put(record(message(i)), 1000L + i);
            }

            int index = 5;
            try (RecordScanner scanner = store.scanFrom(offsets[5])) {
                while (scanner.next()) {
                    assertEquals(offsets[index], scanner.offset());
                    assertEquals(message(index), CODEC.decode(scanner.body()).getMessage());
                    index++;
                }
                assertEquals(RECORDS, index);
            }

            try (RecordScanner tail = store.scanFrom(offsets[RECORDS - 1] + frameLen())) {
                assertFalse("从尾位点开始应无记录", tail.next());
            }
        }
    }

    @Test
    public void scannerDoesNotChaseWritesAfterSnapshot() {
        File dir = dirFor("snapshot");
        StoreConfig config = config(dir);

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC).build()) {
            for (int i = 0; i < RECORDS; i++) {
                store.put(record(message(i)), 1000L + i);
            }

            try (RecordScanner scanner = store.scan()) {
                assertTrue(scanner.next());
                long snapshotEnd = scanner.endOffset();
                store.put(record(message(0)), 2000L);

                int seen = 1;
                while (scanner.next()) {
                    seen++;
                }
                assertEquals("扫描器只覆盖创建时的快照", RECORDS, seen);
                assertEquals(snapshotEnd, scanner.endOffset());
            }
        }
    }

    @Test
    public void forEachDecodesAndStopsWhenVisitorReturnsFalse() {
        File dir = dirFor("forEach");
        StoreConfig config = config(dir);

        try (MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC).build()) {
            for (int i = 0; i < RECORDS; i++) {
                store.put(record(message(i)), 1000L + i);
            }

            List<String> all = new ArrayList<>();
            store.forEach((offset, frame, value) -> {
                all.add(value.getMessage());
                return true;
            });
            assertEquals(RECORDS, all.size());
            assertEquals(message(0), all.get(0));
            assertEquals(message(RECORDS - 1), all.get(RECORDS - 1));

            int[] count = {0};
            store.forEach((offset, frame, value) -> {
                count[0]++;
                return count[0] < 4;
            });
            assertEquals(4, count[0]);
        }
    }

    @Test
    public void scanCanDriveAnOfflineRewriteIntoANewStore() {
        File sourceDir = dirFor("rewrite-source");
        File targetDir = dirFor("rewrite-target");
        StoreConfig sourceConfig = config(sourceDir);
        StoreConfig targetConfig = config(targetDir);

        try (MiniFileStore<LogRecord> source = MiniFileStore.builder(sourceConfig, CODEC).build()) {
            for (int i = 0; i < RECORDS; i++) {
                source.put(record(message(i)), 1000L + i);
            }
        }

        // 业务层 compaction:只保留 tagCode 为偶数的记录,写入全新的 store(offset 从 0 开始)
        int kept = 0;
        try (MiniFileStore<LogRecord> source = MiniFileStore.builder(sourceConfig, CODEC).build();
             MiniFileStore<LogRecord> target = MiniFileStore.builder(targetConfig, CODEC).build();
             RecordScanner scanner = source.scan()) {
            while (scanner.next()) {
                long tag = scanner.frame().getTagCode();
                if ((tag & 1L) == 0L) {
                    target.put(CODEC.decode(scanner.body()), tag);
                    kept++;
                }
            }
            assertEquals(5, kept);
        }

        // 新 store 从 0 开始,内容只剩压缩后的记录
        try (MiniFileStore<LogRecord> target = MiniFileStore.builder(targetConfig, CODEC).build();
             RecordScanner scanner = target.scan()) {
            int count = 0;
            while (scanner.next()) {
                long tag = scanner.frame().getTagCode();
                assertEquals(0L, tag & 1L);
                count++;
            }
            assertEquals(5, count);
        }
    }

    @Test
    public void scanAfterCloseIsRejected() {
        File dir = dirFor("closed");
        StoreConfig config = config(dir);
        MiniFileStore<LogRecord> store = MiniFileStore.builder(config, CODEC).build();
        store.put(record(message(0)), 1L);
        store.close();
        try {
            store.scan();
            fail("关闭后不应允许扫描");
        } catch (IllegalStateException expected) {
            assertTrue(expected.getMessage().contains("closed"));
        }
    }

    private static StoreConfig config(File dir) {
        return StoreConfig.builder(dir.getPath())
                .commitLogFileSize(FILE_SIZE)
                .maxBodySize(MAX_BODY_SIZE)
                .build();
    }

    private static long frameLen() {
        return RecordFrame.HEADER_SIZE + CODEC.encode(record(message(0))).length;
    }

    private static String message(int i) {
        return String.format("rec-%02d-%s", i, "xxxxxxxxx");
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
