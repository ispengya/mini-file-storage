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
     * 是否已被判定为写满:帧装不下时置位。
     * 不再把 wrotePosition 抬到 fileSize——浪费区必须保持"不可读",
     * 否则索引回放会把零字节当成坏帧。
     */
    private volatile boolean full;
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
        return this.full || this.wrotePosition.get() >= this.fileSize;
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
            this.full = true;
            return false;
        }
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        buffer.position(currentPos);
        buffer.put(data, offset, length);
        if (this.wrotePosition.addAndGet(length) >= this.fileSize) {
            this.full = true;
        }
        return true;
    }

    public void setWrotePosition(int position) {
        this.wrotePosition.set(position);
        this.full = position >= this.fileSize;
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
     * <p>本方法只负责定位断点,不清洗尾部:尾部是否合法由调用方按文件角色判断
     * (末文件允许撕裂残尾,非末文件必须全零)。</p>
     *
     * @param maxBodySize bodyLen 合法上限,必须与读路径使用同一个值
     * @return 恢复出的写入位置(最后一条完整帧的结束位置)
     */
    public int recoverRecordStoreWrotePosition(int maxBodySize) {
        int position = 0;
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        while (true) {
            RecordFrame.Frame frame = RecordFrame.parse(
                    buffer, position, this.fileSize - position, maxBodySize);
            if (frame.getStatus() != RecordFrame.Status.OK) {
                break;
            }
            position += frame.getTotalLen();
        }
        setWrotePosition(position);
        this.flushedPosition.set(position);
        return position;
    }

    /**
     * 检查 [position, fileSize) 是否全为零。
     *
     * <p>顺序日志正常滚动时,"装不下下一帧"留下的浪费区本来就是预分配的零字节;
     * 非末文件出现非零尾部,只能是断电残帧或外部损坏,必须按全局前缀截断处理。</p>
     */
    public boolean isTailZero(int position) {
        ByteBuffer buffer = this.mappedByteBuffer.slice();
        for (int i = Math.max(position, 0); i < this.fileSize; i++) {
            if (buffer.get(i) != 0) {
                return false;
            }
        }
        return true;
    }

    /**
     * 把 [position, fileSize) 整段置零并 force(等价于逻辑 truncate)。
     *
     * <p>只清 4KB 不足以覆盖大帧撕裂后的残留;整段清零保证下次恢复不会把残片当数据。</p>
     */
    public void zeroTailFrom(int position) {
        int from = Math.max(position, 0);
        if (from >= this.fileSize) {
            return;
        }
        ByteBuffer probe = this.mappedByteBuffer.slice();
        boolean dirty = false;
        for (int i = from; i < this.fileSize; i++) {
            if (probe.get(i) != 0) {
                dirty = true;
                break;
            }
        }
        if (!dirty) {
            return;
        }
        byte[] zeros = new byte[8192];
        int cursor = from;
        while (cursor < this.fileSize) {
            int len = Math.min(zeros.length, this.fileSize - cursor);
            ByteBuffer buffer = this.mappedByteBuffer.slice();
            buffer.position(cursor);
            buffer.put(zeros, 0, len);
            cursor += len;
        }
        this.mappedByteBuffer.force();
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
        setWrotePosition(position);
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
        setWrotePosition(position);
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
