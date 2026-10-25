package com.ispengya.file;

import com.ispengya.file.api.FileStore;
import com.ispengya.file.api.MiniFileStore;
import com.ispengya.file.api.StoreConfig;
import com.ispengya.file.codec.LogRecordCodec;
import com.ispengya.file.model.LogRecord;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.util.Comparator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

/**
 * 磁盘格式元数据契约:配置一旦落盘,后续打开必须与之完全一致,
 * 不允许通过调小 fileSize/maxBodySize 让 setLength 或恢复扫描静默改写数据。
 */
public class StoreMetadataTest {

    private static final String ROOT = "./target/store-metadata-test";
    private static final LogRecordCodec CODEC = new LogRecordCodec();

    @Before
    public void setUp() {
        deleteRecursively(new File(ROOT));
    }

    @Test
    public void configMismatchIsRejectedBeforeTouchingData() {
        File dir = dirFor("mismatch");
        StoreConfig original = config(dir, 256, 64);
        FileStore<LogRecord> store = new MiniFileStore<>(original, CODEC);
        store.put(record("kept"), 1L);
        store.close();

        assertTrue("首次建库必须写元数据", new File(original.metaPath()).exists());

        expectOpenFailure(config(dir, 512, 64), "commitLogFileSize");
        expectOpenFailure(config(dir, 256, 128), "maxBodySize");

        // 原配置仍可正常打开且数据未被动过
        FileStore<LogRecord> reopened = new MiniFileStore<>(original, CODEC);
        try {
            assertEquals("kept", reopened.getByLogicalIndex(0).getMessage());
        } finally {
            reopened.close();
        }
    }

    @Test
    public void legacyStoreWithoutMetadataIsAdoptedOnlyWithMatchingFileSize() throws Exception {
        File dir = dirFor("legacy");
        StoreConfig original = config(dir, 256, 64);
        FileStore<LogRecord> store = new MiniFileStore<>(original, CODEC);
        store.put(record("legacy"), 1L);
        store.close();

        // 模拟旧版本建的库:没有 store.properties
        assertTrue(new File(original.metaPath()).delete());

        FileStore<LogRecord> adopted = new MiniFileStore<>(original, CODEC);
        try {
            assertEquals("legacy", adopted.getByLogicalIndex(0).getMessage());
            assertTrue("旧库应按当前配置补写元数据", new File(original.metaPath()).exists());
        } finally {
            adopted.close();
        }

        // 再删元数据,用不同文件尺寸打开:旧数据文件长度校验必须拒绝
        assertTrue(new File(original.metaPath()).delete());
        expectOpenFailure(config(dir, 512, 64), "length");
    }

    @Test
    public void corruptMetadataIsRejected() throws Exception {
        File dir = dirFor("corrupt-meta");
        StoreConfig config = config(dir, 256, 64);
        new MiniFileStore<>(config, CODEC).close();

        Files.write(new File(config.metaPath()).toPath(),
                "format.version=oops\n".getBytes(StandardCharsets.UTF_8));

        expectOpenFailure(config, "format.version");
    }

    private static StoreConfig config(File dir, int fileSize, int maxBodySize) {
        return StoreConfig.builder(dir.getPath())
                .commitLogFileSize(fileSize)
                .maxBodySize(maxBodySize)
                .build();
    }

    private static void expectOpenFailure(StoreConfig config, String messageFragment) {
        try {
            FileStore<LogRecord> store = new MiniFileStore<>(config, CODEC);
            store.close();
            fail("配置与磁盘布局不一致时必须拒绝打开");
        } catch (IllegalStateException e) {
            assertTrue("异常信息应包含 " + messageFragment + ", 实际: " + e.getMessage(),
                    e.getMessage().contains(messageFragment));
        }
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
