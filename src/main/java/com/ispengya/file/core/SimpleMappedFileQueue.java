package com.ispengya.file.core;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * 简化版映射文件队列
 * 管理同一目录下的一组顺序文件，负责文件滚动与统一刷盘
 */
public class SimpleMappedFileQueue {
    /**
     * 存储目录
     */
    private final String storePath;
    /**
     * 单个文件大小
     */
    private final int fileSize;
    /**
     * 已创建的映射文件列表，按起始偏移有序
     */
    private final List<SimpleMappedFile> mappedFiles = new ArrayList<>();

    public SimpleMappedFileQueue(String storePath, int fileSize) {
        this.storePath = storePath;
        this.fileSize = fileSize;
        SimpleMappedFile.ensureDirOK(storePath);
    }

    public static SimpleMappedFileQueue recoverForRecordStore(String storePath, int fileSize) throws IOException {
        SimpleMappedFileQueue queue = new SimpleMappedFileQueue(storePath, fileSize);
        File dir = new File(storePath);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return queue;
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            long startOffset;
            try {
                startOffset = Long.parseLong(file.getName());
            } catch (NumberFormatException e) {
                continue;
            }
            SimpleMappedFile mappedFile = new SimpleMappedFile(file.getPath(), fileSize, startOffset);
            int wrote = mappedFile.recoverRecordStoreWrotePosition();
            if (wrote > 0 || queue.mappedFiles.isEmpty()) {
                queue.mappedFiles.add(mappedFile);
            } else {
                mappedFile.close();
            }
        }
        return queue;
    }

    public static SimpleMappedFileQueue recoverForConsumeQueue(String storePath, int fileSize) throws IOException {
        SimpleMappedFileQueue queue = new SimpleMappedFileQueue(storePath, fileSize);
        File dir = new File(storePath);
        File[] files = dir.listFiles();
        if (files == null || files.length == 0) {
            return queue;
        }
        Arrays.sort(files, Comparator.comparing(File::getName));
        for (File file : files) {
            if (!file.isFile()) {
                continue;
            }
            long startOffset;
            try {
                startOffset = Long.parseLong(file.getName());
            } catch (NumberFormatException e) {
                continue;
            }
            SimpleMappedFile mappedFile = new SimpleMappedFile(file.getPath(), fileSize, startOffset);
            int wrote = mappedFile.recoverConsumeQueueWrotePosition();
            if (wrote > 0 || queue.mappedFiles.isEmpty()) {
                queue.mappedFiles.add(mappedFile);
            } else {
                mappedFile.close();
            }
        }
        return queue;
    }

    /**
     * 获取最后一个映射文件，必要时创建新文件
     *
     * @param createIfAbsent true 且不存在时创建新文件
     */
    public synchronized SimpleMappedFile getLastMappedFile(boolean createIfAbsent) throws IOException {
        SimpleMappedFile mappedFile;
        if (mappedFiles.isEmpty()) {
            if (!createIfAbsent) {
                return null;
            }
            long startOffset = 0;
            String fileName = storePath + File.separator + formatFileName(startOffset);
            mappedFile = new SimpleMappedFile(fileName, fileSize, startOffset);
            mappedFiles.add(mappedFile);
            return mappedFile;
        } else {
            mappedFile = mappedFiles.get(mappedFiles.size() - 1);
            if (mappedFile.isFull() && createIfAbsent) {
                long startOffset = mappedFile.getFileFromOffset() + fileSize;
                String fileName = storePath + File.separator + formatFileName(startOffset);
                mappedFile = new SimpleMappedFile(fileName, fileSize, startOffset);
                mappedFiles.add(mappedFile);
            }
            return mappedFile;
        }
    }

    /**
     * 根据全局物理偏移找到对应的映射文件
     *
     * @param offset 全局物理偏移
     * @return 包含该偏移的文件，找不到时返回 null
     */
    public synchronized SimpleMappedFile findMappedFileByOffset(long offset) {
        if (mappedFiles.isEmpty()) {
            return null;
        }
        for (SimpleMappedFile mappedFile : mappedFiles) {
            long fileStart = mappedFile.getFileFromOffset();
            long fileEnd = fileStart + mappedFile.getWrotePosition();
            if (offset >= fileStart && offset < fileEnd) {
                return mappedFile;
            }
        }
        return null;
    }

    /**
     * 将物理偏移格式化成固定长度文件名
     * 方便按字典序排序即是按偏移排序
     */
    private String formatFileName(long offset) {
        return String.format("%020d", offset);
    }

    /**
     * 获取当前队列中最大物理偏移量
     */
    public synchronized long getMaxOffset() {
        if (mappedFiles.isEmpty()) {
            return 0;
        }
        SimpleMappedFile last = mappedFiles.get(mappedFiles.size() - 1);
        return last.getFileFromOffset() + last.getWrotePosition();
    }

    /**
     * 刷新所有文件到磁盘
     *
     * @return 最后一个文件刷盘后的最大偏移
     */
    public synchronized long flush() {
        long flushed = 0;
        for (SimpleMappedFile mappedFile : mappedFiles) {
            flushed = mappedFile.getFileFromOffset() + mappedFile.flush();
        }
        return flushed;
    }

    /**
     * 关闭所有文件并清理列表
     */
    public synchronized void close() {
        for (SimpleMappedFile mappedFile : mappedFiles) {
            mappedFile.close();
        }
        mappedFiles.clear();
    }
}
