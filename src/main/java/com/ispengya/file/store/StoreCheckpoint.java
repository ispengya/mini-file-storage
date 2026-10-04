package com.ispengya.file.store;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * 双槽原子检查点(v2)
 *
 * <p>记录三类水位,用于恢复加速、Reput 起点提示与索引交叉校验锚。
 * 单槽直写存在"半写槽"风险(16B 覆写切在中间),v2 改为 A/B 两槽交替:
 * 每次 save 只重写"较旧"的那个槽并递增 seq,任何时刻至少一槽完整;
 * load 校验两槽(magic + CRC),取 seq 大的有效槽。</p>
 *
 * <p>槽布局(44 字节,大端):</p>
 * <pre>
 * 0  4   magic      0x43503201("CP2")
 * 4  4   seq        单调递增序号,新槽写 max(seq)+1
 * 8  8   logFlushMax  CommitLog 已 flush 的最大物理 offset
 * 16 8   cqFlushMax     ConsumeQueue 文件已 flush 的最大字节 offset(-1 表示无 CQ)
 * 24 8   cqCursor       Reput 已构建索引到的日志位点(-1 表示不适用)
 * 32 8   keyCursor      Reput 已构建 KeyIndex 到的日志位点(-1 表示不适用)
 * 40 4   crc          覆盖前 40 字节
 * </pre>
 *
 * <p>不变式:cqCursor ≤ cqFlushMax 对应位点 ≤ logFlushMax 的数据存在性由写入顺序保证
 * (数据先落、水位最后写),所以 checkpoint 永远"滞后但真实"。</p>
 */
public class StoreCheckpoint {

    /**
     * 槽魔数,兼作格式版本
     */
    static final int SLOT_MAGIC = 0x43503201;

    /**
     * 单槽字节数:magic+seq+4×long+crc
     */
    static final int SLOT_SIZE = 44;

    /**
     * CRC 覆盖的头部字节数(槽内除 crc 自身)
     */
    private static final int CRC_COVER_SIZE = SLOT_SIZE - 4;

    /**
     * 文件总长:两个槽
     */
    static final int FILE_SIZE = SLOT_SIZE * 2;

    private final File file;

    public StoreCheckpoint(String filePath) {
        this.file = new File(filePath);
    }

    /**
     * 写一条新水位:选 seq 落后的槽整体覆写,force(true) 同步元数据
     */
    public synchronized void save(long logFlushMax, long cqFlushMax, long cqCursor, long keyCursor) {
        Slot slotA = readSlot(0);
        Slot slotB = readSlot(1);

        // 写"序号落后"的槽(两槽皆空时视为同 0,写 A);新 seq = max+1 → 任何时刻至少一槽完整
        int seqA = slotA == null ? 0 : slotA.seq;
        int seqB = slotB == null ? 0 : slotB.seq;
        int targetIndex = seqA <= seqB ? 0 : 1;
        int nextSeq = Math.max(seqA, seqB) + 1;

        ByteBuffer buffer = ByteBuffer.allocate(SLOT_SIZE);
        buffer.putInt(SLOT_MAGIC);
        buffer.putInt(nextSeq);
        buffer.putLong(logFlushMax);
        buffer.putLong(cqFlushMax);
        buffer.putLong(cqCursor);
        buffer.putLong(keyCursor);
        CRC32 crc = new CRC32();
        crc.update(buffer.array(), 0, CRC_COVER_SIZE);
        buffer.putInt((int) crc.getValue());

        File parent = file.getParentFile();
        if (parent != null && !parent.exists()) {
            parent.mkdirs();
        }
        try (RandomAccessFile raf = new RandomAccessFile(file, "rw")) {
            raf.setLength(FILE_SIZE);
            raf.seek((long) targetIndex * SLOT_SIZE);
            raf.write(buffer.array());
            raf.getChannel().force(true);
        } catch (IOException e) {
            throw new RuntimeException("save checkpoint failed: " + file.getPath(), e);
        }
    }

    /**
     * 读取最新有效水位
     *
     * @return {@code long[]{logFlushMax, cqFlushMax, cqCursor, keyCursor}};
     *         文件缺失/旧 16B 格式/两槽全部校验失败时返回 null(调用方退化为文件自推导)
     */
    public synchronized long[] load() {
        Slot slotA = readSlot(0);
        Slot slotB = readSlot(1);
        Slot newest = null;
        if (slotA != null && slotB != null) {
            newest = slotA.seq >= slotB.seq ? slotA : slotB;
        } else if (slotA != null) {
            newest = slotA;
        } else {
            newest = slotB;
        }
        if (newest == null) {
            return null;
        }
        return new long[]{newest.logFlushMax, newest.cqFlushMax, newest.cqCursor, newest.keyCursor};
    }

    /**
     * 读取并校验单个槽
     *
     * @return 校验通过的槽内容;magic/CRC 不符或文件不足长时返回 null
     */
    private Slot readSlot(int index) {
        try {
            if (!file.exists() || file.length() < FILE_SIZE) {
                return null;
            }
            byte[] raw = new byte[SLOT_SIZE];
            try (RandomAccessFile raf = new RandomAccessFile(file, "r")) {
                raf.seek((long) index * SLOT_SIZE);
                int read = raf.read(raw);
                if (read < SLOT_SIZE) {
                    return null;
                }
            }
            ByteBuffer buffer = ByteBuffer.wrap(raw);
            if (buffer.getInt(0) != SLOT_MAGIC) {
                return null;
            }
            CRC32 crc = new CRC32();
            crc.update(raw, 0, CRC_COVER_SIZE);
            if (buffer.getInt(CRC_COVER_SIZE) != (int) crc.getValue()) {
                return null;
            }
            return new Slot(buffer.getInt(4), buffer.getLong(8), buffer.getLong(16),
                    buffer.getLong(24), buffer.getLong(32));
        } catch (IOException e) {
            return null;
        }
    }

    /**
     * 槽内容值对象
     */
    private static final class Slot {
        final int seq;
        final long logFlushMax;
        final long cqFlushMax;
        final long cqCursor;
        final long keyCursor;

        Slot(int seq, long logFlushMax, long cqFlushMax, long cqCursor, long keyCursor) {
            this.seq = seq;
            this.logFlushMax = logFlushMax;
            this.cqFlushMax = cqFlushMax;
            this.cqCursor = cqCursor;
            this.keyCursor = keyCursor;
        }
    }
}
