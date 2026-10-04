package com.ispengya.file;

import com.ispengya.file.core.RecordFrame;
import com.ispengya.file.core.RecordFrame.Frame;
import com.ispengya.file.core.RecordFrame.Status;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Random;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertSame;

/**
 * 记录帧 v2 编解码的纯逻辑测试:往返、三态判定、脏值防御
 */
public class RecordFrameTest {

    private static final int MAX_BODY = 1024 * 1024;

    private static Frame parseAtStart(byte[] frame) {
        return RecordFrame.parse(ByteBuffer.wrap(frame), 0, frame.length, MAX_BODY);
    }

    @Test
    public void encodeParseRoundTrip() {
        byte[] body = "hello-frame".getBytes();
        byte[] frame = RecordFrame.encode(1234L, 5678L, 99L, body);

        Frame parsed = parseAtStart(frame);
        assertEquals(Status.OK, parsed.getStatus());
        assertEquals(RecordFrame.HEADER_SIZE + body.length, parsed.getTotalLen());
        assertEquals(1234L, parsed.getStoreTimestamp());
        assertEquals(5678L, parsed.getTagCode());
        assertEquals(99L, parsed.getKeyHash());

        // body 原样保留在帧尾
        byte[] extracted = Arrays.copyOfRange(frame, RecordFrame.HEADER_SIZE, frame.length);
        assertArrayEquals(body, extracted);
    }

    @Test
    public void emptyBodyIsValid() {
        byte[] frame = RecordFrame.encode(1L, 2L, 3L, new byte[0]);
        Frame parsed = parseAtStart(frame);
        assertEquals(Status.OK, parsed.getStatus());
        assertEquals(RecordFrame.HEADER_SIZE, parsed.getTotalLen());
    }

    @Test
    public void anyBodyBitFlipIsCorrupted() {
        byte[] body = new byte[64];
        new Random(42).nextBytes(body);
        for (int i = 0; i < body.length; i++) {
            byte[] frame = RecordFrame.encode(10L, 20L, 30L, body);
            frame[RecordFrame.HEADER_SIZE + i] ^= 0x01;
            assertSame("body 第 " + i + " 字节翻转必须判为 CORRUPTED",
                    Status.CORRUPTED, parseAtStart(frame).getStatus());
        }
    }

    @Test
    public void headerFieldFlipIsCorrupted() {
        // storeTimestamp / tagCode / keyHash / CRC 自身被篡改:magic 完好,CRC 必须抓住
        int[] offsets = {8, 16, 24, 32};
        for (int off : offsets) {
            byte[] frame = RecordFrame.encode(1L, 2L, 3L, "x".getBytes());
            frame[off] ^= 0x40;
            assertSame("偏移 " + off + " 翻转必须判为 CORRUPTED",
                    Status.CORRUPTED, parseAtStart(frame).getStatus());
        }
    }

    @Test
    public void magicFlipIsNotMatched() {
        byte[] frame = RecordFrame.encode(1L, 2L, 3L, "x".getBytes());
        frame[0] ^= 0x01;
        assertSame(Status.NOT_MATCHED, parseAtStart(frame).getStatus());
    }

    @Test
    public void zeroHoleIsNotMatched() {
        byte[] zeros = new byte[RecordFrame.HEADER_SIZE];
        assertSame(Status.NOT_MATCHED,
                RecordFrame.parse(ByteBuffer.wrap(zeros), 0, zeros.length, MAX_BODY).getStatus());
    }

    @Test
    public void insufficientSpaceIsNotMatched() {
        byte[] frame = RecordFrame.encode(1L, 2L, 3L, new byte[100]);
        assertSame(Status.NOT_MATCHED,
                RecordFrame.parse(ByteBuffer.wrap(frame), 0, RecordFrame.HEADER_SIZE - 1, MAX_BODY).getStatus());
        // body 被截断:声明的 bodyLen 放不下,视为不完整帧
        assertSame(Status.NOT_MATCHED,
                RecordFrame.parse(ByteBuffer.wrap(frame), 0, frame.length - 1, MAX_BODY).getStatus());
    }

    @Test
    public void bogusBodyLenIsNotMatched() {
        byte[] frame = RecordFrame.encode(1L, 2L, 3L, new byte[10]);
        // 篡改 bodyLen 但不碰 magic(模拟头部部分撕裂):上限检查必须在 CRC 之前拦住
        ByteBuffer.wrap(frame).putInt(4, Integer.MAX_VALUE / 2);
        assertSame(Status.NOT_MATCHED, parseAtStart(frame).getStatus());

        ByteBuffer.wrap(frame).putInt(4, -5);
        assertSame(Status.NOT_MATCHED, parseAtStart(frame).getStatus());
    }

    @Test
    public void maxBodyLenBoundaryRespected() {
        byte[] body = new byte[64];
        byte[] frame = RecordFrame.encode(1L, 1L, 1L, body);
        // 上限恰好等于 bodyLen:合法
        assertSame(Status.OK, RecordFrame.parse(ByteBuffer.wrap(frame), 0, frame.length, 64).getStatus());
        // 上限小 1 字节:判为脏值
        assertSame(Status.NOT_MATCHED, RecordFrame.parse(ByteBuffer.wrap(frame), 0, frame.length, 63).getStatus());
    }

    @Test
    public void parseUsesAbsoluteOffset() {
        byte[] first = RecordFrame.encode(1L, 10L, 100L, "one".getBytes());
        byte[] second = RecordFrame.encode(2L, 20L, 200L, "two".getBytes());
        byte[] both = new byte[first.length + second.length];
        System.arraycopy(first, 0, both, 0, first.length);
        System.arraycopy(second, 0, both, first.length, second.length);

        ByteBuffer buffer = ByteBuffer.wrap(both);
        Frame f1 = RecordFrame.parse(buffer, 0, both.length, MAX_BODY);
        Frame f2 = RecordFrame.parse(buffer, f1.getTotalLen(), both.length - f1.getTotalLen(), MAX_BODY);
        assertEquals(Status.OK, f1.getStatus());
        assertEquals(Status.OK, f2.getStatus());
        assertEquals(10L, f1.getTagCode());
        assertEquals(20L, f2.getTagCode());
        assertEquals(2L, f2.getStoreTimestamp());
    }

    @Test
    public void crcIsDeterministicAndFieldSensitive() {
        byte[] a = RecordFrame.encode(7L, 7L, 7L, "same".getBytes());
        byte[] b = RecordFrame.encode(7L, 7L, 7L, "same".getBytes());
        assertArrayEquals(a, b);
        assertNotEquals(
                RecordFrame.computeCrc(a, 0, 4),
                RecordFrame.computeCrc(RecordFrame.encode(7L, 7L, 7L, "samp".getBytes()), 0, 4));
    }
}
