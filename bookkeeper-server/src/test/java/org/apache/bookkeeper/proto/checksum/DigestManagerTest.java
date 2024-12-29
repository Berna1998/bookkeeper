package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorImpl;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.Collection;

import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

@RunWith(value = Parameterized.class)
public class DigestManagerTest {
    private DigestManager dM;
    private DigestManager dM2;
    private long entryId;
    private long ledgerId;
    private byte[] passw;
    private DataFormats.LedgerMetadataFormat.DigestType digestType;
    private ByteBufAllocator bufAll;
    private boolean v2Protocol;
    private int lastAdd;
    private int length;
    private int flags;
    private ByteBuf buffer;
    private byte[] masterK;


    public DigestManagerTest(int lastAdd, int length, ByteBuf buffer, byte[] masterK, int flags, long entryId,long ledgerId, byte[] passw, DataFormats.LedgerMetadataFormat.DigestType digestType, ByteBufAllocator bufAll, boolean v2Protocol){
        this.lastAdd = lastAdd;
        this.length = length;
        this.buffer = buffer;
        this.masterK = masterK;
        this.flags = flags;
        this.entryId = entryId;
        this.ledgerId = ledgerId;
        this.passw = passw;
        this.digestType = digestType;
        this.bufAll = bufAll;
        this.v2Protocol = v2Protocol;
    }


    @Parameterized.Parameters
    public static Collection returnParams() {
        byte[] masterKe = "provaprovaprovaprova".getBytes();
        ByteBuf buffMock = mock(ByteBuf.class);
        ByteBuf buffUnpool;
        byte[] dat = new byte[10];
        buffUnpool = Unpooled.buffer(1024);
        buffUnpool.writeLong(1);
        buffUnpool.writeLong(2);
        buffUnpool.writeLong(4);
        buffUnpool.writeLong(10);
        buffUnpool.writeBytes(dat);


        ByteBuf buffUnpool2;
        byte[] dat2 = new byte[10];
        buffUnpool2 = Unpooled.buffer(1024);
        buffUnpool2.writeLong(1);
        buffUnpool2.writeLong(2);
        buffUnpool2.writeLong(4);
        buffUnpool2.writeLong(10);
        buffUnpool2.writeBytes(dat2);



        ByteBuf buffSmallEn = Unpooled.buffer(16900);
        byte[] datas = new byte[16385];
        buffSmallEn.writeBytes(datas);

        return Arrays.asList(new Object[][] {

                {1,30, buffUnpool,masterKe,5,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT,false},
                {1,30, buffUnpool,masterKe,5,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,UnpooledByteBufAllocator.DEFAULT,false},
                {1,30, buffUnpool,masterKe,5,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32C,UnpooledByteBufAllocator.DEFAULT,false},
                {4,30, buffUnpool,masterKe,5,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,UnpooledByteBufAllocator.DEFAULT,false},

                {1,30, buffUnpool,masterKe,5,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT,true},

                {1,10, buffMock,masterKe,5,1,1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,ByteBufAllocatorImpl.DEFAULT,true},

                {1,30, buffMock,null,5,1,1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32C,ByteBufAllocatorImpl.DEFAULT,false},
                {1,30, buffMock,null,5,1,-1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,null,false},
                {1,20, buffSmallEn,masterKe,5,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT,true},

                {1,30, buffSmallEn,null,5,-1,-1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,null,true},
                {1,30, buffMock,null,5,0,-1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,null,false},

                {0,-1, buffUnpool2,masterKe,0,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,UnpooledByteBufAllocator.DEFAULT,false},
                {-1,0, buffUnpool2,null,-1,1,0,null, DataFormats.LedgerMetadataFormat.DigestType.CRC32C,ByteBufAllocatorImpl.DEFAULT,false},





        });
    }

    @Test
    public void empty(){

    }
}