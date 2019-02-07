package de.mpicbg.ulman.imgstreamer;

import java.io.*;

import net.imagej.ImgPlus;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import org.zeromq.ZMQ;

import static de.mpicbg.ulman.imgstreamer.testStreams.fillImg;

public class testZMQ
{
	public static void main(String... args)
	{
		/*
		System.out.println("-------------------------------------------------");
		testNonBufferedReadOut();

		System.out.println("-------------------------------------------------");
		testBufferedReadOut();
		*/

		System.out.println("-------------------------------------------------");
		testByteArray2ImageTransfer(new UnsignedShortType());
	}


	public static void testNonBufferedReadOut()
	{
		try {
			ZMQ.Context zmqContext = ZMQ.context(1);
			ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.connect("tcp://localhost:3456");

			byte[] testSendArray = new byte[] { 97,120,98,99,100,97,98,99,100,97,98,99 };
			zmqSocket.send(testSendArray);

			final ZeroMQInputStream zis = new ZeroMQInputStream(3456, 5);
			int nextVal = 0;
			do {
				nextVal = zis.read();
				System.out.println("got now this value: "+nextVal);

				if (nextVal == 120)
				{
					//modify the buffer to recognize it...
					for (int i=0; i < testSendArray.length; ++i) testSendArray[i]+=4;
					zmqSocket.send(testSendArray);
				}
			} while (nextVal > -1);

			zmqSocket.close();
			zis.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}


	public static void testBufferedReadOut()
	{
		try {
			ZMQ.Context zmqContext = ZMQ.context(1);
			ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.connect("tcp://localhost:3456");

			byte[] testSendArray = new byte[] { 97,120,98,99,100,97,98,99,100,97,98,99 };
			zmqSocket.send(testSendArray);

			byte[] testRecvArray = new byte[5];

			final ZeroMQInputStream zis = new ZeroMQInputStream(3456, 10);
			int nextVal = 0;
			do {
				int itemsRead = zis.read(testRecvArray);
				nextVal = itemsRead;
				System.out.println("itemsRead="+itemsRead);

				for (int q=0; q < itemsRead; ++q)
				{
					nextVal = testRecvArray[q];
					System.out.println("got now this value: "+nextVal);

					if (nextVal == 120)
					{
						//modify the buffer to recognize it...
						for (int i=0; i < testSendArray.length; ++i) testSendArray[i]+=4;
						zmqSocket.send(testSendArray);
					}
				}
			} while (nextVal > -1);

			zmqSocket.close();
			zis.close();
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}


	static <T extends RealType<T> & NativeType<T>>
	void testByteArray2ImageTransfer(final T type)
	{
		Img<T> img
			= fillImg( new ArrayImgFactory(type).create(200,100,5) );

		try {
			//stream out a real image into a byte[]
			final ImgPlus<T> imgP = new ImgPlus<>(img);
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			ImgStreamer isv = new ImgStreamer( new testStreams.myLogger() );
			isv.setImageForStreaming(imgP);
			System.out.println("stream length will be: "+isv.getOutputStreamLength());
			isv.write(os);

			ZMQ.Context zmqContext = ZMQ.context(1);
			ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.connect("tcp://localhost:3456");
			zmqSocket.send(os.toByteArray());

			final ZeroMQInputStream zis = new ZeroMQInputStream(3456, 10);
			ImgPlus<? extends RealType<?>> imgPP = isv.readAsRealTypedImg(zis);

			zmqSocket.close();
			zis.close();

			System.out.println("got this image: "+imgPP.getImg().toString()
			                  +" of "+imgPP.getImg().firstElement().getClass().getSimpleName());

			Cursor<T> cP = imgP.getImg().cursor();
			cP.jumpFwd(50);

			Cursor<? extends RealType<?>> cPP = imgPP.getImg().cursor();
			cPP.jumpFwd(50);

			if (cP.get().getRealDouble() != cPP.get().getRealDouble())
				System.out.println("----------> PIXEL VALUES MISMATCH! <----------");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
	}
}
