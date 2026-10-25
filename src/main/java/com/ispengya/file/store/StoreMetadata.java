package com.ispengya.file.store;

import com.ispengya.file.api.StoreConfig;

import java.io.File;
import java.io.IOException;
import java.io.Reader;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Properties;

/**
 * 磁盘格式元数据(store.properties)
 *
 * <p>首次建库时把影响磁盘布局的配置落盘,后续每次打开强校验。目的:</p>
 * <ul>
 *   <li>配置调小 commitLogFileSize 时,{@code SimpleMappedFile.setLength(fileSize)}
 *       会静默截断已有数据文件——打开前直接拒绝,绝不动盘;</li>
 *   <li>配置调小 maxBodySize 时,恢复扫描与读取使用同一上限,不会出现
 *       "恢复判合法、读取报非法"的分裂;</li>
 *   <li>格式版本显式落盘,为后续磁盘格式升级留出拒绝旧版本的锚点。</li>
 * </ul>
 *
 * <p>元数据缺失(旧版本建的库)时按当前配置补写,但会先校验已有数据文件长度与
 * commitLogFileSize 一致;不一致仍然拒绝启动。</p>
 */
public final class StoreMetadata {

    /**
     * 当前磁盘格式版本
     */
    public static final int FORMAT_VERSION = 1;

    private static final String KEY_VERSION = "format.version";
    private static final String KEY_COMMIT_LOG_FILE_SIZE = "commitLogFileSize";
    private static final String KEY_CONSUME_QUEUE_FILE_SIZE = "consumeQueueFileSize";
    private static final String KEY_KEY_INDEX_FILE_SIZE = "keyIndexFileSize";
    private static final String KEY_MAX_BODY_SIZE = "maxBodySize";

    private StoreMetadata() {
    }

    /**
     * 校验现有 store.properties 与配置一致;文件不存在时按当前配置创建
     *
     * @param config 本次打开使用的配置
     * @throws IllegalStateException 元数据损坏、版本不支持、配置与磁盘布局不一致
     */
    public static synchronized void verifyOrCreate(StoreConfig config) {
        File metaFile = new File(config.metaPath());
        if (metaFile.exists()) {
            verify(config, metaFile, load(metaFile));
            verifyExistingCommitLogFileSize(config);
        } else {
            // 旧库兼容:先确认现有数据文件长度与当前配置匹配,再补写元数据
            verifyExistingCommitLogFileSize(config);
            write(config, metaFile);
        }
    }

    /**
     * 读取并校验元数据文件
     */
    private static Properties load(File metaFile) {
        Properties properties = new Properties();
        try (Reader reader = Files.newBufferedReader(metaFile.toPath(), StandardCharsets.UTF_8)) {
            properties.load(reader);
            return properties;
        } catch (IOException e) {
            throw new IllegalStateException("failed to read store metadata: " + metaFile.getPath(), e);
        }
    }

    /**
     * 逐项比对配置与磁盘布局
     */
    private static void verify(StoreConfig config, File metaFile, Properties properties) {
        int version = intValue(metaFile, properties, KEY_VERSION);
        if (version != FORMAT_VERSION) {
            throw new IllegalStateException("unsupported store format version " + version
                    + " in " + metaFile.getPath() + " (expected " + FORMAT_VERSION + ")");
        }
        checkValue(metaFile, properties, KEY_COMMIT_LOG_FILE_SIZE, config.getCommitLogFileSize());
        checkValue(metaFile, properties, KEY_CONSUME_QUEUE_FILE_SIZE, config.getConsumeQueueFileSize());
        checkValue(metaFile, properties, KEY_KEY_INDEX_FILE_SIZE, config.getKeyIndexFileSize());
        checkValue(metaFile, properties, KEY_MAX_BODY_SIZE, config.getMaxBodySize());
    }

    /**
     * 单项差异直接拒绝启动,避免 setLength 静默改写磁盘布局
     */
    private static void checkValue(File metaFile, Properties properties, String key, int expected) {
        int actual = intValue(metaFile, properties, key);
        if (actual != expected) {
            throw new IllegalStateException("store config mismatch in " + metaFile.getPath() + ": "
                    + key + " is " + actual + " on disk but " + expected + " in current config; "
                    + "reopen with the original StoreConfig or migrate the store");
        }
    }

    /**
     * 现有 CommitLog 数据文件都是预分配到固定长度的;长度变化说明配置曾变动或文件被改写
     */
    private static void verifyExistingCommitLogFileSize(StoreConfig config) {
        File[] files = new File(config.commitLogPath()).listFiles();
        if (files == null) {
            return;
        }
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            try {
                Long.parseLong(file.getName());
            } catch (NumberFormatException e) {
                continue;
            }
            long actual = file.length();
            if (actual != config.getCommitLogFileSize()) {
                throw new IllegalStateException("commitlog file " + file.getPath() + " has length "
                        + actual + " but commitLogFileSize is " + config.getCommitLogFileSize()
                        + "; opening would rewrite the on-disk layout — use the original config or migrate");
            }
        }
    }

    /**
     * 读取整型属性,缺失或脏值都视为元数据损坏
     */
    private static int intValue(File metaFile, Properties properties, String key) {
        String raw = properties.getProperty(key);
        if (raw == null) {
            throw new IllegalStateException("store metadata missing '" + key + "': " + metaFile.getPath());
        }
        try {
            return Integer.parseInt(raw.trim());
        } catch (NumberFormatException e) {
            throw new IllegalStateException("invalid '" + key + "' in store metadata: " + metaFile.getPath(), e);
        }
    }

    /**
     * 原子写入元数据(临时文件 + 同目录 move),避免首次建库时留下半截 properties
     */
    private static void write(StoreConfig config, File metaFile) {
        StringBuilder content = new StringBuilder();
        content.append("# mini-file-storage on-disk format metadata; do not edit\n");
        content.append(KEY_VERSION).append('=').append(FORMAT_VERSION).append('\n');
        content.append(KEY_COMMIT_LOG_FILE_SIZE).append('=').append(config.getCommitLogFileSize()).append('\n');
        content.append(KEY_CONSUME_QUEUE_FILE_SIZE).append('=').append(config.getConsumeQueueFileSize()).append('\n');
        content.append(KEY_KEY_INDEX_FILE_SIZE).append('=').append(config.getKeyIndexFileSize()).append('\n');
        content.append(KEY_MAX_BODY_SIZE).append('=').append(config.getMaxBodySize()).append('\n');

        Path target = metaFile.toPath();
        Path tmp = null;
        try {
            tmp = Files.createTempFile(target.getParent(), ".store", ".meta.tmp");
            Files.write(tmp, content.toString().getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(tmp, target, StandardCopyOption.ATOMIC_MOVE);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            tmp = null;
        } catch (IOException e) {
            throw new IllegalStateException("failed to write store metadata: " + metaFile.getPath(), e);
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                }
            }
        }
    }
}
