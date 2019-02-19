/*
 * To the extent possible under law, the ImageJ developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
package de.mpicbg.ulman.imgstreamer;

import java.io.*;

import net.imagej.ImgPlus;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import org.zeromq.ZMQ;

import static de.mpicbg.ulman.imgstreamer.testStreams.areBothImagesTheSame;
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

		System.out.println("-------------------------------------------------");
		testImage2ByteArrayTransfer(new UnsignedShortType());

		System.out.println("-------------------------------------------------");
		testImage2Image_ConcurrentThreads(new UnsignedShortType());
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

		ZMQ.Socket zmqSocket = null;
		ZeroMQInputStream zis = null;

		try {
			// -------- path outwards --------
			//stream out a real image into a byte[]
			final ImgPlus<T> imgP = new ImgPlus<>(img);
			final ByteArrayOutputStream os = new ByteArrayOutputStream();
			ImgStreamer isv = new ImgStreamer( new testStreams.myLogger() );
			isv.setImageForStreaming(imgP);
			System.out.println("stream length will be: "+isv.getOutputStreamLength());
			isv.write(os);

			ZMQ.Context zmqContext = ZMQ.context(1);
			zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.connect("tcp://localhost:3456");
			zmqSocket.send(os.toByteArray());

			// -------- path inwards --------
			zis = new ZeroMQInputStream(3456, 10);
			ImgPlus<? extends RealType<?>> imgPP = isv.readAsRealTypedImg(zis);

			// -------- testing --------
			System.out.println("got this image: "+imgPP.getImg().toString()
			                  +" of "+imgPP.getImg().firstElement().getClass().getSimpleName());
			System.out.println("--> send and receive images are the same: "
				+areBothImagesTheSame(imgP,(ImgPlus)imgPP) +"\n");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (zmqSocket != null) zmqSocket.close();
			if (zis != null) zis.close();
		}
	}


	static <T extends RealType<T> & NativeType<T>>
	void testImage2ByteArrayTransfer(final T type)
	{
		Img<T> img
			= fillImg( new ArrayImgFactory(type).create(200,100,5) );

		ZMQ.Socket zmqSocket = null;
		ZeroMQOutputStream zos = null;

		System.out.println("--> this test should fail complaining about \"no confirmation detected\"");

		try {
			// -------- path outwards --------
			final ImgPlus<T> imgP = new ImgPlus<>(img);
			ImgStreamer isv = new ImgStreamer( new testStreams.myLogger() );
			isv.setImageForStreaming(imgP);
			System.out.println("stream length will be: "+isv.getOutputStreamLength());

			//zos = new ZeroMQOutputStream(3456, 10);
			zos = new ZeroMQOutputStream("tcp://localhost:3456", 10);
			isv.write(zos);
			System.out.println("finito sending");

			// -------- path inwards --------
			//stream in a real image into a byte[]
			ZMQ.Context zmqContext = ZMQ.context(1);
			zmqSocket = zmqContext.socket(ZMQ.PAIR);
			//zmqSocket.connect("tcp://localhost:3456");
			zmqSocket.bind("tcp://*:3456");

			final ByteArrayInputStream is = new ByteArrayInputStream( zmqSocket.recv() );
			System.out.println("got byte buffer of length: "+is.available());
			ImgPlus<? extends RealType<?>> imgPP = isv.readAsRealTypedImg(is);

			zos.close();
			zmqSocket.close();

			// -------- testing --------
			System.out.println("got this image: "+imgPP.getImg().toString()
			                  +" of "+imgPP.getImg().firstElement().getClass().getSimpleName());
			System.out.println("--> send and receive images are the same: "
				+areBothImagesTheSame(imgP,(ImgPlus)imgPP) +"\n");
		}
		catch (IOException e) {
			e.printStackTrace();
		}
		finally {
			if (zmqSocket != null) zmqSocket.close();
			if (zos != null) zos.close();
		}
	}


	static <T extends RealType<T> & NativeType<T>>
	void testImage2Image_ConcurrentThreads(final T type)
	{
		final ImgPlus<T> imgP
			= new ImgPlus<>( fillImg( new ArrayImgFactory(type).create(200,100,5) ) );

		class LocalSender extends Thread
		{
			private boolean shouldBindInsteadOfConnect = false;
			public LocalSender(boolean _shouldBindInsteadOfConnect)
			{
				shouldBindInsteadOfConnect = _shouldBindInsteadOfConnect;
			}

			@Override
			public void run()
			{
				try {
					ImgStreamer isv = new ImgStreamer( new testStreams.myLogger() );

					final ZeroMQOutputStream zos
						= shouldBindInsteadOfConnect ? new ZeroMQOutputStream(3456, 10)
						: new ZeroMQOutputStream("tcp://localhost:3456", 10);

					isv.setImageForStreaming(imgP);
					System.out.println("sender: stream length will be: "+isv.getOutputStreamLength());
					isv.write(zos);

					System.out.println("sender: finito sending");
					zos.close();
				}
				catch (IOException e) {
					System.out.println("sender problem:");
					e.printStackTrace();
				}
			}
		}

		class LocalReceiver extends Thread
		{
			ImgPlus<? extends RealType<?>> imgPP;

			private boolean shouldBindInsteadOfConnect = true;
			public LocalReceiver(boolean _shouldBindInsteadOfConnect)
			{
				shouldBindInsteadOfConnect = _shouldBindInsteadOfConnect;
			}

			@Override
			public void run()
			{
				try {
					ImgStreamer isv = new ImgStreamer( new testStreams.myLogger() );

					final ZeroMQInputStream zis
						= shouldBindInsteadOfConnect ? new ZeroMQInputStream(3456, 10)
						: new ZeroMQInputStream("tcp://localhost:3456", 10);

					imgPP = isv.readAsRealTypedImg(zis);

					System.out.println("receiver: finito receiving");
					zis.close();
				}
				catch (IOException e) {
					System.out.println("receiver problem:");
					e.printStackTrace();
				}
			}
		}

		try {
			LocalSender   lsend = new LocalSender(false);   //connect
			LocalReceiver lrecv = new LocalReceiver(true);  //bind

			LocalSender   lserv = new LocalSender(true);    //bind
			LocalReceiver lreq  = new LocalReceiver(false); //connect

			//push model
			lsend.start();
			Thread.sleep(2000);
			lrecv.start();

			lsend.join();
			lrecv.join();
			System.out.println("--> send and receive images are the same: "
				+areBothImagesTheSame(imgP,(ImgPlus)lrecv.imgPP) +"\n");

			//pull model
			lserv.start();
			Thread.sleep(2000);
			lreq.start();

			lserv.join();
			lreq.join();
			System.out.println("--> send and receive images are the same: "
				+areBothImagesTheSame(imgP,(ImgPlus)lreq.imgPP) +"\n");




			//the threads are created again...
			lsend = new LocalSender(false);   //connect
			lrecv = new LocalReceiver(true);  //bind

			lserv = new LocalSender(true);    //bind
			lreq  = new LocalReceiver(false); //connect

			//push model but receiver is started first
			lrecv.start();
			Thread.sleep(2000);
			lsend.start();

			lsend.join();
			lrecv.join();
			System.out.println("--> send and receive images are the same: "
				+areBothImagesTheSame(imgP,(ImgPlus)lrecv.imgPP) +"\n");

			//pull model but requester is started first
			lreq.start();
			Thread.sleep(2000);
			lserv.start();

			lserv.join();
			lreq.join();
			System.out.println("--> send and receive images are the same: "
				+areBothImagesTheSame(imgP,(ImgPlus)lreq.imgPP) +"\n");

		} catch (InterruptedException e) {
			e.printStackTrace();
		}

		//ImgPlus<? extends RealType<?>> imgPP = lrecv.imgPP;
	}
}
