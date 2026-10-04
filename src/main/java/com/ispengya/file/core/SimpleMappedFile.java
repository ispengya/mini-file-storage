package com.ispengya.file.core;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * 简化版内存映射文件
 * 封装单个固定大小文件，提供顺序写入和刷盘能力
 */
public class SimpleMappedFile {
    /**
     * 文件路径
     */
    private final String fileName;
    /**
     * 文件大小
     */
    private final int fileSize;
    /**
     * 底层文件通道
     */
    private final FileChannel fileChannel;
    /**
     * 映射到内存的缓冲区
     */
    private final MappedByteBuffer mappedByteBuffer;
    /**
     * 已写入的位置
     */
    private final AtomicInteger wrotePosition = new AtomicInteger(0);
    /**
     * 已刷盘的位置
     */
    private final AtomicInteger flushedPosition = new AtomicInteger(0);
    /**
     * 该文件起始物理偏移量
     */
    private final long fileFromOffset;

    public SimpleMappedFile(String fileName, int fileSize, long fileFromOffset) throws IOException {
        this.fileName = fileName;
        this.fileSize = fileSize;
        this.fileFromOffset = fileFromOffset;
        ensureDirOK(new File(fileName).getParent());
        RandomAccessFile raf = new RandomAccessFile(fileName, "rw");
        raf.setLength(fileSize);
        this.fileChannel = raf.getChannel();
        this.mappedByteBuffer = fileChannel.map(FileChannel.MapMode.READ_WRITE, 0, fileSize);
    }

    /**
     * 确保目录存在，不存在则创建
     */
    public static void ensureDirOK(String dirName) {
        if (dirName == null) {
            return;
        }
        File dir = new File(dirName);
        if (!dir.exists()) {
            dir.mkdirs();
        }
    }

    /**
     * 当前文件是否已写满
     */
    public boolean isFull() {
        return this.wrotePosition.get() >= this.fileSize;
    }

    public long getFileFromOffset() {
        return fileFromOffset;
    }

    public int getWrotePosition() {
        return wrotePosition.get();
    }

    public int getFlushedPosition() {
        return flushedPosition.get();
    }

    /**
     * 在当前写入位置顺序追加一段数据
     *
     * @return true 表示写入成功，false 表示空间不足
     */
    public boolean append(byte[] data, int offset, int length) {
        int currentPos = this.wrotePosition.get();
        if (currentPos + length > this.fileSize) {
            this.wrotePosition.set(this.fileSize);
            return false;
        }
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        buffer.position(currentPos);
        buffer.put(data, offset, length);
        this.wrotePosition.addAndGet(length);
        return true;
    }

    public void setWrotePosition(int position) {
        this.wrotePosition.set(position);
    }

    public void setFlushedPosition(int position) {
        this.flushedPosition.set(position);
    }

    /**
     * 按 RecordFrame(v2)协议扫描恢复写入位置
     *
     * <p>逐帧解析:合法帧推进整帧;NOT_MATCHED(非帧起点/空间不足/bodyLen 脏值)
     * 或 CORRUPTED(magic 完好但 CRC 不符,即断电页撕裂)均停止推进——
     * 这是与 v1"只看 length 结构"的本质区别:内容损坏不再被静默放行。</p>
     *
     * <p>停止后对 [停点, 停点+4KB) 做脏尾清除:存在非零字节则整段置零并立即 force,
     * 避免半条记录残留盘上(Java 无法对 mmap 中的文件安全 truncate,置零等效)。</p>
     *
     * @return 恢复出的写入位置(最后一条完整帧的结束位置)
     */
    public int recoverRecordStoreWrotePosition() {
        int position = 0;
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        while (true) {
            RecordFrame.Frame frame = RecordFrame.parse(
                    buffer, position, this.fileSize - position, Integer.MAX_VALUE);
            if (frame.getStatus() != RecordFrame.Status.OK) {
                break;
            }
            position += frame.getTotalLen();
        }
        this.wrotePosition.set(position);
        this.flushedPosition.set(position);

        int clearTo = Math.min(position + 4096, this.fileSize);
        boolean dirtyTail = false;
        for (int i = position; i < clearTo; i++) {
            if (buffer.get(i) != 0) {
                dirtyTail = true;
                break;
            }
        }
        if (dirtyTail) {
            for (int i = position; i < clearTo; i++) {
                buffer.put(i, (byte) 0);
            }
            this.mappedByteBuffer.force();
        }
        return position;
    }

    public int recoverConsumeQueueWrotePosition() {
        int position = 0;
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        while (position + 20 <= this.fileSize) {
            buffer.position(position);
            long physicalOffset = buffer.getLong();
            int size = buffer.getInt();
            if (physicalOffset < 0 || size <= 0) {
                break;
            }
            position += 20;
        }
        this.wrotePosition.set(position);
        this.flushedPosition.set(position);
        return position;
    }

    /**
     * 扫描 KeyIndex 文件恢复写入位置
     * 按 16 字节 [keyHash][physicalOffset] 单元逐条校验:
     * 遇到全零空洞或非法 physicalOffset 即停止,后续追加从有效区尾部继续
     *
     * @return 恢复出的写入位置
     */
    public int recoverKeyIndexWrotePosition() {
        int position = 0;
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        while (position + 16 <= this.fileSize) {
            buffer.position(position);
            long keyHash = buffer.getLong();
            long physicalOffset = buffer.getLong();
            if (keyHash == 0L && physicalOffset == 0L) {
                break;
            }
            if (physicalOffset < 0) {
                break;
            }
            position += 16;
        }
        this.wrotePosition.set(position);
        this.flushedPosition.set(position);
        return position;
    }

    /**
     * 从指定文件内偏移位置读取一段数据，不改变写入位置
     *
     * @param position 文件内起始偏移
     * @param length   读取长度
     * @return 包含读取数据的 ByteBuffer，position 为 0，limit 为 length
     */
    public ByteBuffer read(int position, int length) {
        int maxReadable = this.wrotePosition.get();
        if (position < 0 || length < 0 || position + length > maxReadable) {
            throw new IllegalArgumentException("read range out of wrote position");
        }
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        buffer.position(position);
        ByteBuffer result = ByteBuffer.allocate(length);
        for (int i = 0; i < length; i++) {
            result.put(buffer.get());
        }
        result.flip();
        return result;
    }

    /**
     * 将当前写入位置之前的数据刷到磁盘
     *
     * @return 刷盘到的文件内偏移
     */
    public int flush() {
        int value = this.wrotePosition.get();
        if (value > this.flushedPosition.get()) {
            this.mappedByteBuffer.force();
            this.flushedPosition.set(value);
        }
        return this.flushedPosition.get();
    }

    /**
     * 关闭底层文件通道
     */
    public void close() {
        try {
            this.fileChannel.close();
        } catch (IOException ignored) {
        }
    }

    public String getFileName() {
        return fileName;
    }

    public int getFileSize() {
        return fileSize;
    }
}
