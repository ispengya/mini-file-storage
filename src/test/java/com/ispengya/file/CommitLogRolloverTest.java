package com.ispengya.file;

import com.ispengya.file.api.FileStore;
import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.RandomAccessFile;
import java.nio.file.Files;
import java.util.Comparator;
import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * CommitLog 文件滚动与截断后续写的门面级回归:
 *
 * <ol>
 *   <li>SYNC 模式下跨文件写入必须正常——索引游标落在封存文件的浪费区时要能跳到下一个文件，
 *       既不能把零字节当坏帧，也不能卡死;</li>
 *   <li>全局前缀截断后，门面仍能在断点继续 put，逻辑索引立即可见。</li>
 * </ol>
 */
public class CommitLogRolloverTest {

    private static final String ROOT = "./target/commitlog-rollover-test";
    private static final int FILE_SIZE = 256;
    private static final int MAX_BODY_SIZE = 64;
    private static final int RECORDS = 10;
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void syncPutCrossesFileBoundaryAndIndexesAllRecords() {
        File dir = dirFor("rollover");
        StoreConfig config = config(dir);
        FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
        try {
            for (int i = 0; i < RECORDS; i++) {
                store.put(record(message(i)), 1000L + i);
            }
            // SYNC 模式下每条 put 返回即索引可见;跨文件写入若卡在浪费区,这里会先失败
            for (int i = 0; i < RECORDS; i++) {
                assertEquals(message(i), store.getByLogicalIndex(i).getMessage());
            }
        } finally {
            store.close();
        }

        // 重启后逻辑索引完整
        FileStore<LogRecord> reopened = new MiniFileStore<>(config, CODEC);
        try {
            for (int i = 0; i < RECORDS; i++) {
                assertEquals(message(i), reopened.getByLogicalIndex(i).getMessage());
            }
        } finally {
            reopened.close();
        }
    }

    @Test
    public void storeRemainsWritableAfterGlobalTruncation() throws Exception {
        File dir = dirFor("truncate-then-write");
        StoreConfig config = config(dir);
        FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
        for (int i = 0; i < RECORDS; i++) {
            store.put(record(message(i)), 1000L + i);
        }
        store.close();

        // 抹掉中间封存文件首帧 magic → 全局前缀在 256 处截断
        corruptFirstFrame(new File(config.commitLogPath(), "00000000000000000256"));

        MiniFileStore<LogRecord> reopened = new MiniFileStore<>(config, CODEC);
        try {
            assertEquals(256L, reopened.getRecoveryTruncatedOffset());
            for (int i = 0; i < 3; i++) {
                assertEquals(message(i), reopened.getByLogicalIndex(i).getMessage());
            }
            assertNull("截断区之后逻辑索引必须不可见", reopened.getByLogicalIndex(3));

            String fresh = message(0);
            long newOffset = reopened.put(record(fresh), 2000L);
            assertEquals("续写必须从截断点开始", 256L, newOffset);
            assertEquals(fresh, reopened.get(newOffset).getMessage());
            assertEquals("截断后续写的索引必须立即可见", fresh, reopened.getByLogicalIndex(3).getMessage());
        } finally {
            reopened.close();
        }

        // 隔离已落盘:再次打开不再报截断,截断前与截断后的记录都在
        MiniFileStore<LogRecord> again = new MiniFileStore<>(config, CODEC);
        try {
            assertEquals(-1L, again.getRecoveryTruncatedOffset());
            for (int i = 0; i < 3; i++) {
                assertEquals(message(i), again.getByLogicalIndex(i).getMessage());
            }
            assertEquals(message(0), again.getByLogicalIndex(3).getMessage());
        } finally {
            again.close();
        }
    }

    private static StoreConfig config(File dir) {
        return StoreConfig.builder(dir.getPath())
                .commitLogFileSize(FILE_SIZE)
                .maxBodySize(MAX_BODY_SIZE)
                .build();
    }

    private static String message(int i) {
        return String.format("rec-%02d-%s", i, "xxxxxxxxx");
    }

    private static LogRecord record(String message) {
        return new LogRecord(1000L, "INFO", message);
    }

    private static void corruptFirstFrame(File file) throws Exception {
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.seek(0);
            raf.write(new byte[4]);
            raf.getChannel().force(true);
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
            java.util.stream.Stream<java.nio.file.Path> paths = Files.walk(file.toPath())
                    .sorted(Comparator.reverseOrder());
            List<java.nio.file.Path> list = paths.collect(java.util.stream.Collectors.toList());
            paths.close();
            for (java.nio.file.Path path : list) {
                try {
                    Files.delete(path);
                } catch (Exception ignored) {
                }
            }
        } catch (Exception ignored) {
        }
    }
}
