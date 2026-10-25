package com.ispengya.file.index;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;

/**
 * 索引游标文件:用一个独立小文件持久化"已覆盖到的 CommitLog 物理位点"。
 *
 * <p>约定"先数据、后游标":调用方必须先 flush 索引数据,再 {@link #save(long)};
 * 这样崩溃最多退回旧游标重放,不会出现"游标已越过、数据还没落盘"的永久缺口。</p>
 *
 * <p>写入采用同目录临时文件 + 原子替换,读取失败/缺失/脏值统一返回 -1,
 * 调用方据此退化为全量回放。</p>
 */
public final class IndexCursorFile {

    private final File file;

    public IndexCursorFile(File file) {
        if (file == null) {
            throw new IllegalArgumentException("cursor file must not be null");
        }
        this.file = file;
    }

    public IndexCursorFile(String path) {
        this(new File(path));
    }

    /**
     * 读取游标
     *
     * @return 非负位点;文件缺失、内容为空或无法解析时返回 -1
     */
    public synchronized long load() {
        if (!file.exists()) {
            return -1L;
        }
        try {
            String text = new String(Files.readAllBytes(file.toPath()), StandardCharsets.UTF_8).trim();
            return text.isEmpty() ? -1L : Long.parseLong(text);
        } catch (IOException | NumberFormatException e) {
            return -1L;
        }
    }

    /**
     * 原子写入游标(临时文件 + 同目录 move)
     *
     * @param cursor 已覆盖到的物理位点
     */
    public synchronized void save(long cursor) {
        Path target = file.toPath();
        Path tmp = null;
        try {
            Path parent = target.toAbsolutePath().getParent();
            if (parent == null) {
                throw new IOException("cursor file has no parent directory: " + file);
            }
            Files.createDirectories(parent);
            tmp = Files.createTempFile(parent, ".index-cursor", ".tmp");
            Files.write(tmp, Long.toString(cursor).getBytes(StandardCharsets.UTF_8));
            try {
                Files.move(tmp, target,
                        StandardCopyOption.ATOMIC_MOVE, StandardCopyOption.REPLACE_EXISTING);
            } catch (AtomicMoveNotSupportedException e) {
                Files.move(tmp, target, StandardCopyOption.REPLACE_EXISTING);
            }
            tmp = null;
        } catch (IOException e) {
            throw new IllegalStateException("failed to persist index cursor: " + file, e);
        } finally {
            if (tmp != null) {
                try {
                    Files.deleteIfExists(tmp);
                } catch (IOException ignored) {
                }
            }
        }
    }

    /**
     * 删除游标文件(下次启动退化为全量回放)
     */
    public synchronized void clear() {
        try {
            Files.deleteIfExists(file.toPath());
        } catch (IOException ignored) {
        }
    }

    public File getFile() {
        return file;
    }
}
