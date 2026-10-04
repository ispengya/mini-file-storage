package com.ispengya.file.core;

import java.nio.ByteBuffer;
import java.util.zip.CRC32;

/**
 * CommitLog 记录帧编解码(格式 v2),帧格式知识的唯一来源
 *
 * <p>磁盘帧布局(全部大端):</p>
 * <pre>
 * 偏移  长度  字段
 * 0     4     magic          0x484F544B("HOTK"),兼作格式版本标识
 * 4     4     bodyLen        Codec 编码后的 payload 字节数
 * 8     8     storeTimestamp 引擎盖章的写入时间(服务端接收语义)
 * 16    8     tagCode        业务 tag,与写入 ConsumeQueue 的值同源
 * 24    8     keyHash        业务 key 的 32 位无符号 hash,0 表示无 key
 * 32    4     bodyCRC        CRC32,覆盖 magic..keyHash 与 body,不含自身
 * 36    var   body           Codec&lt;T&gt; 编码产物
 * </pre>
 *
 * <p>设计原则:物理层(SimpleMappedFile/MmapSequentialLog)不感知格式,
 * 组帧与校验只经过本类;恢复扫描通过 {@link #parse} 判定帧边界。</p>
 *
 * <p>{@link Status} 三态语义:</p>
 * <ul>
 *   <li>OK — magic、bodyLen、CRC 全部合法,可按 totalLen 推进</li>
 *   <li>NOT_MATCHED — 不是帧起点(magic 不符/空间不足/bodyLen 非法),恢复扫描在此停止</li>
 *   <li>CORRUPTED — 帧结构合法但内容损坏(magic 与 CRC 都在却对不上),
 *       典型场景是断电导致的页间撕裂;恢复必须停在其之前,读取必须抛错,绝不放行</li>
 * </ul>
 */
public final class RecordFrame {

    /**
     * 帧魔数:"HOTK"(Hot key / Key storage 自嘲式命名),大端写入
     */
    public static final int MAGIC = 0x484F544B;

    /**
     * 定长帧头大小(magic 到 bodyCRC)
     */
    public static final int HEADER_SIZE = 36;

    /**
     * CRC 字段在帧头内的偏移,校验范围不含自身及 body 之外内容
     */
    private static final int CRC_FIELD_OFFSET = 32;

    /**
     * 帧解析状态
     */
    public enum Status {
        /**
         * 合法帧
         */
        OK,
        /**
         * 此处不是帧起点/数据不足,扫描应停止并视为空洞
         */
        NOT_MATCHED,
        /**
         * 帧头结构合法但 CRC 不匹配,内容已损坏
         */
        CORRUPTED
    }

    /**
     * 解析结果值对象
     */
    public static final class Frame {

        private final Status status;
        private final int bodyLen;
        private final int totalLen;
        private final long storeTimestamp;
        private final long tagCode;
        private final long keyHash;

        private Frame(Status status, int bodyLen, int totalLen,
                      long storeTimestamp, long tagCode, long keyHash) {
            this.status = status;
            this.bodyLen = bodyLen;
            this.totalLen = totalLen;
            this.storeTimestamp = storeTimestamp;
            this.tagCode = tagCode;
            this.keyHash = keyHash;
        }

        public Status getStatus() {
            return status;
        }

        /**
         * payload 字节数;NOT_MATCHED 时无意义(0)
         */
        public int getBodyLen() {
            return bodyLen;
        }

        /**
         * 帧总长(HEADER_SIZE + bodyLen),仅 OK 有效
         */
        public int getTotalLen() {
            return totalLen;
        }

        /**
         * 引擎写入时间,仅 OK 有效
         */
        public long getStoreTimestamp() {
            return storeTimestamp;
        }

        /**
         * 业务 tag,仅 OK 有效
         */
        public long getTagCode() {
            return tagCode;
        }

        /**
         * 业务 key hash(0=无),仅 OK 有效
         */
        public long getKeyHash() {
            return keyHash;
        }
    }

    private static final Frame NOT_MATCHED_FRAME =
            new Frame(Status.NOT_MATCHED, 0, 0, 0L, 0L, 0L);

    private RecordFrame() {
    }

    /**
     * 组帧:头字段 + body + 回填 CRC
     *
     * @param body Codec 编码产物,长度受调用方 maxBodySize 约束
     * @return 完整帧字节,可直接交给 SequentialLog.append
     */
    public static byte[] encode(long storeTimestamp, long tagCode, long keyHash, byte[] body) {
        if (body == null) {
            throw new IllegalArgumentException("body must not be null");
        }
        byte[] frame = new byte[HEADER_SIZE + body.length];
        ByteBuffer buffer = ByteBuffer.wrap(frame);
        buffer.putInt(MAGIC);
        buffer.putInt(body.length);
        buffer.putLong(storeTimestamp);
        buffer.putLong(tagCode);
        buffer.putLong(keyHash);
        buffer.putInt(0); // CRC 占位,计算完成后回填
        buffer.put(body);
        buffer.putInt(CRC_FIELD_OFFSET, computeCrc(frame, 0, body.length));
        return frame;
    }

    /**
     * 从字节缓冲区的绝对位置解析帧(不改变 buffer 的 position)
     *
     * @param buffer     源数据(通常是 mmap 的 slice)
     * @param at         帧起始绝对下标
     * @param available  从 at 起可读的字节数(恢复扫描传 fileSize - at)
     * @param maxBodyLen bodyLen 合法上限(防脏值导致巨帧误判)
     */
    public static Frame parse(ByteBuffer buffer, int at, int available, int maxBodyLen) {
        if (available < HEADER_SIZE) {
            return NOT_MATCHED_FRAME;
        }
        if (buffer.getInt(at) != MAGIC) {
            return NOT_MATCHED_FRAME;
        }
        int bodyLen = buffer.getInt(at + 4);
        if (bodyLen < 0 || bodyLen > maxBodyLen || bodyLen > available - HEADER_SIZE) {
            return NOT_MATCHED_FRAME;
        }

        CRC32 crc = new CRC32();
        byte[] headerPart = new byte[CRC_FIELD_OFFSET];
        for (int i = 0; i < CRC_FIELD_OFFSET; i++) {
            headerPart[i] = buffer.get(at + i);
        }
        crc.update(headerPart);
        if (bodyLen > 0) {
            byte[] bodyPart = new byte[bodyLen];
            for (int i = 0; i < bodyLen; i++) {
                bodyPart[i] = buffer.get(at + HEADER_SIZE + i);
            }
            crc.update(bodyPart);
        }
        int storedCrc = buffer.getInt(at + CRC_FIELD_OFFSET);
        if (storedCrc != (int) crc.getValue()) {
            return new Frame(Status.CORRUPTED, bodyLen, HEADER_SIZE + bodyLen, 0L, 0L, 0L);
        }
        return new Frame(Status.OK, bodyLen, HEADER_SIZE + bodyLen,
                buffer.getLong(at + 8), buffer.getLong(at + 16), buffer.getLong(at + 24));
    }

    /**
     * 计算已组好帧的 CRC:覆盖 [frameStart, frameStart+32) 头部与随后的 body
     */
    public static int computeCrc(byte[] frame, int frameStart, int bodyLen) {
        CRC32 crc = new CRC32();
        crc.update(frame, frameStart, CRC_FIELD_OFFSET);
        if (bodyLen > 0) {
            crc.update(frame, frameStart + HEADER_SIZE, bodyLen);
        }
        return (int) crc.getValue();
    }
}
