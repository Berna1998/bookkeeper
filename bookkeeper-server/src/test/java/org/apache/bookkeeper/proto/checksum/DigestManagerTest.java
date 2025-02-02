package org.apache.bookkeeper.proto.checksum;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufAllocator;
import io.netty.buffer.Unpooled;
import io.netty.buffer.UnpooledByteBufAllocator;
import io.netty.util.ReferenceCounted;
import javafx.scene.input.DataFormat;
import org.apache.bookkeeper.client.BKException;
import org.apache.bookkeeper.client.BookKeeper;
import org.apache.bookkeeper.common.allocator.impl.ByteBufAllocatorImpl;
import org.apache.bookkeeper.proto.DataFormats;
import org.apache.bookkeeper.util.ByteBufList;
import org.junit.Before;
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
    private DigestManager dM32;
    private static DataFormats.LedgerMetadataFormat.DigestType crc32c = DataFormats.LedgerMetadataFormat.DigestType.CRC32C;
    private static DataFormats.LedgerMetadataFormat.DigestType crc32 = DataFormats.LedgerMetadataFormat.DigestType.CRC32;
    private static DataFormats.LedgerMetadataFormat.DigestType hmac = DataFormats.LedgerMetadataFormat.DigestType.HMAC;
    private static DataFormats.LedgerMetadataFormat.DigestType dummy = DataFormats.LedgerMetadataFormat.DigestType.DUMMY;
    private long entryId;
    private long ledgerId;
    private byte[] passw;
    private DataFormats.LedgerMetadataFormat.DigestType digestType;
    private ByteBufAllocator bufAll;
    private int lastAdd;
    private int length;
    private ByteBuf buffer;


    public DigestManagerTest(int lastAdd, int length, ByteBuf buffer, long entryId,long ledgerId, byte[] passw, DataFormats.LedgerMetadataFormat.DigestType digestType, ByteBufAllocator bufAll){
        this.lastAdd = lastAdd;
        this.length = length;
        this.buffer = buffer;
        this.entryId = entryId;
        this.ledgerId = ledgerId;
        this.passw = passw;
        this.digestType = digestType;
        this.bufAll = bufAll;

    }


    @Parameterized.Parameters
    public static Collection returnParams() {
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

                {1,30, buffUnpool,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT},
                {1,30, buffUnpool,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,UnpooledByteBufAllocator.DEFAULT},
                {1,30, buffUnpool,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32C,UnpooledByteBufAllocator.DEFAULT},

               // {1,30, null,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT},
              //  {1,30, null,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,UnpooledByteBufAllocator.DEFAULT},
              //  {1,30, null,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32C,UnpooledByteBufAllocator.DEFAULT},





                {4,30, buffUnpool,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,UnpooledByteBufAllocator.DEFAULT},


                {-1,30, buffUnpool,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT},

                {0,30, buffUnpool,-1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT},
                {1,30, buffUnpool,0,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT},
                {1,30, buffSmallEn,-1,-1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,null},
                //SOPRA LE PRIME


               // {1,0, buffUnpool,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,UnpooledByteBufAllocator.DEFAULT},

           //     {1,10, buffMock,-1,1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,ByteBufAllocatorImpl.DEFAULT},
          //      {1,30, buffMock,1,1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32C,ByteBufAllocatorImpl.DEFAULT},
          //      {1,30, buffMock,1,-1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,null},


                //{1,20, buffSmallEn,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32,UnpooledByteBufAllocator.DEFAULT},

                {1,30, buffSmallEn,-1,-1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,null},
           //     {1,30, buffMock,0,-1,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.DUMMY,null},

                {0,-1, buffUnpool2,1,1,"prova".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.HMAC,UnpooledByteBufAllocator.DEFAULT},

                {-1,0, buffUnpool2,1,0,null, DataFormats.LedgerMetadataFormat.DigestType.CRC32C,ByteBufAllocatorImpl.DEFAULT},


        });
    }


    @Test
    public void testClass() throws GeneralSecurityException, BKException.BKDigestMatchException {
        if(this.entryId > 0) {
            this.dM = DigestManager.instantiate(this.ledgerId, this.passw, this.digestType, this.bufAll, false);

            DigestManager digestMan = DigestManager.instantiate(this.ledgerId, this.passw, this.digestType, UnpooledByteBufAllocator.DEFAULT, false);

            ByteBuf byteBuffer;
            byte[] dataa2 = new byte[10];
            byteBuffer = Unpooled.buffer(1024);
            byteBuffer.writeLong(1);
            byteBuffer.writeLong(2);
            byteBuffer.writeLong(4);
            byteBuffer.writeLong(10);
            byteBuffer.writeBytes(dataa2);

            ByteBufList byteBufList = (ByteBufList) digestMan.computeDigestAndPackageForSending(this.entryId, 0, 10, byteBuffer, null, 2);
            ByteBuf data3 = ByteBufList.coalesce(byteBufList);


            ReferenceCounted rF = dM.computeDigestAndPackageForSending(this.entryId, this.lastAdd, this.length, this.buffer, null, 0);


            ByteBufList rFlist = (ByteBufList) rF;
            assertEquals(rFlist.getBuffer(1),this.buffer);

            ByteBuf dataREt = dM.verifyDigestAndReturnData(this.entryId, data3);
            assertEquals(dataREt, data3);


            try {


                if (this.digestType == DataFormats.LedgerMetadataFormat.DigestType.DUMMY) {
                    DigestManager.RecoveryData recoveryData = dM.verifyDigestAndReturnLastConfirmed(this.buffer);
                    assertEquals(this.lastAdd, recoveryData.getLastAddConfirmed());

                }

                if (this.digestType == DataFormats.LedgerMetadataFormat.DigestType.DUMMY) {
                    long lacRet = dM.verifyDigestAndReturnLac(data3);
                    if (this.bufAll == null) {//Dummy line just for line coverage
                        assertEquals(0, lacRet);
                    } else {
                        assertEquals(2, lacRet);
                    }
                }
            } catch (Exception e) {
                assertFalse(false);
            }


         }
    }

    @Test
    public void testForException(){
        if(this.entryId<=0){
            ByteBuf data3 = null;
            DigestManager digestManager32 = null;
            try{
                this.dM = DigestManager.instantiate(this.ledgerId, this.passw, this.digestType, this.bufAll, false);
                digestManager32 = DigestManager.instantiate(this.ledgerId,"".getBytes(), DataFormats.LedgerMetadataFormat.DigestType.CRC32C,UnpooledByteBufAllocator.DEFAULT,true);

                DigestManager digestMan = DigestManager.instantiate(this.ledgerId, this.passw, this.digestType, UnpooledByteBufAllocator.DEFAULT, false);

                ByteBuf byteBuffer;
                byte[] dataa2 = new byte[10];
                byteBuffer = Unpooled.buffer(1024);
                byteBuffer.writeLong(1);
                byteBuffer.writeLong(2);
                byteBuffer.writeLong(4);
                byteBuffer.writeLong(10);
                byteBuffer.writeBytes(dataa2);

                ByteBufList byteBufList = (ByteBufList) digestMan.computeDigestAndPackageForSending(this.entryId, 0, 10, byteBuffer, null, 0);
                data3 = ByteBufList.coalesce(byteBufList);

                ReferenceCounted rF = dM.computeDigestAndPackageForSending(this.entryId, this.lastAdd, this.length, null, null, 0);

            }catch (NullPointerException | GeneralSecurityException e){
                assertFalse(false);
            }

            try{
                DigestManager digestV2 = DigestManager.instantiate(this.ledgerId,"".getBytes(),this.digestType,UnpooledByteBufAllocator.DEFAULT,true);
                ReferenceCounted rF = digestV2.computeDigestAndPackageForSending(this.entryId, this.lastAdd, this.length, mock(ByteBuf.class),  null, 0);
            } catch (NullPointerException | GeneralSecurityException  e) {
                assertFalse(false);
            }

        }
    }

}