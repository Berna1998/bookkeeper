package org.apache.bookkeeper.bookie.storage.ldb;

import io.netty.buffer.*;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.util.Arrays;
import java.util.Collection;


import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class ReadCacheTest {
    private ReadCache rC;
    private final ByteBufAllocator buf;
    private final long maxCacheSize;
    private final int count;
    private final int size;
    private final int inBuf;
    private int ledgerId;
    private ByteBuf buffer;
    private int entryId;

    public ReadCacheTest(int ledgerId,int entryId, ByteBufAllocator buf, long maxCacheSize, int count, int size, int inBuf){
        this.ledgerId = ledgerId;
        this.entryId = entryId;
        this.buf= buf;
        this.maxCacheSize = maxCacheSize;
        this.count = count;
        this.size = size;
        this.inBuf = inBuf;
    }

    @Parameterized.Parameters
    public static Collection returnParams() {
        return Arrays.asList(new Object[][] {

                {1,1,UnpooledByteBufAllocator.DEFAULT,10000,1,128,100},
                {1,1,null,1,0,0,100},
                {1,1,mock(ByteBufAllocator.class),50,0,0,100},
                {1,1,UnpooledByteBufAllocator.DEFAULT, 1024 * 1024 * 1 *1024, 1, 1024, 1024},
                {0,-1,null,1,0,0,100},
                {0,0,mock(ByteBufAllocator.class),50,0,0,100},
                {-1,1,UnpooledByteBufAllocator.DEFAULT,10000,1,128,100},
                {1,1,UnpooledByteBufAllocator.DEFAULT,0,0,0,100},
                {1,1,UnpooledByteBufAllocator.DEFAULT,-1,0,0,100},

        });
    }

    @Before
    public void beforeTest(){
        this.rC = new ReadCache(this.buf, this.maxCacheSize);
    }

    @Test
    public void testClass(){
        if(this.ledgerId>=0) {
            ByteBuf buffer = Unpooled.wrappedBuffer(new byte[this.inBuf]);
            rC.put(this.ledgerId, this.entryId, buffer);

            assertEquals(this.count, rC.count());
            assertEquals(this.size, rC.size());

            ByteBuf find = rC.get(this.ledgerId, this.entryId);
            if (find != null) {
                assertEquals(find, buffer);
            } else {
                assertNull(find);
            }
            boolean entry = rC.hasEntry(this.ledgerId, this.entryId);

            if (this.buf == UnpooledByteBufAllocator.DEFAULT && rC.count()>0) {
                assertTrue(entry);
            } else {
                assertFalse(entry);
            }
            rC.close();

        }else{
            ByteBuf buffer = Unpooled.wrappedBuffer(new byte[this.inBuf]);
            try{
                rC.put(this.ledgerId, this.entryId, buffer);
            } catch (IllegalArgumentException e) {
                assertFalse(false);
            }

            try{
                rC.get(this.ledgerId, this.entryId);
            } catch (IllegalArgumentException e) {
                assertFalse(false);
            }

            try{
                boolean res = rC.hasEntry(this.ledgerId, this.entryId);
            } catch (IllegalArgumentException  e) {
                assertFalse(false);
            }
        }


    }

    @Test
    public void testPutWithRollover() {
        try {
            ByteBuf buffer = Unpooled.buffer(this.size+ 1);
            buffer.writeBytes(new byte[this.size+ 1]);

            rC.put(this.ledgerId, this.entryId, buffer);
            rC.put(this.ledgerId, this.entryId, buffer);
            rC.put(this.ledgerId, this.entryId, buffer);

            ByteBuf result = rC.get(this.ledgerId, this.entryId);

            if(this.buf == null) {
                assertNull(result);
            }

        } catch (NullPointerException | IllegalArgumentException e) {
            assertFalse(false);
        }
    }


}