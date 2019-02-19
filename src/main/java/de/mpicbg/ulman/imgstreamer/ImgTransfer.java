/*
 * To the extent possible under law, the ImageJ developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
package de.mpicbg.ulman.imgstreamer;

import net.imagej.ImgPlus;
import net.imglib2.type.NativeType;
import java.io.IOException;

/**
 * This class provides convenience, front-end functions for ImgPlus transfer.
 */
public class ImgTransfer
{
	// ------------------ static, single-image handling functions ------------------
	// ------------------ send + receive vatiants ------------------

	/**
	 * Sends/pushes an image over network to someone who is receiving it.
	 * Logging/reporting IS supported here whenever \e log != null.
	 */
	public static <T extends NativeType<T>>
	void sendImage(final ImgPlus<T> imgP, final String addr,
	               final int timeOut, final ProgressCallback log)
	throws IOException
	{
		final ZeroMQOutputStream zos = new ZeroMQOutputStream("tcp://"+addr, timeOut);

		final ImgStreamer is = new ImgStreamer(log);
		is.setImageForStreaming(imgP);
		is.write(zos);

		zos.close();
	}

	/**
	 * Sends/pushes an image over network to someone who is receiving it.
	 * No logging/reporting is supported here.
	 */
	public static <T extends NativeType<T>>
	void sendImage(final ImgPlus<T> imgP, final String addr,
	               final int timeOut)
	throws IOException
	{ sendImage(imgP, addr, timeOut, null); }

	/**
	 * Sends/pushes an image over network to someone who is receiving it.
	 * No logging/reporting is supported here, default 30 secs timeout is used.
	 */
	public static <T extends NativeType<T>>
	void sendImage(final ImgPlus<T> imgP, final String addr)
	throws IOException
	{ sendImage(imgP, addr, 30, null); }


	/**
	 * Receives an image over network from someone who is sending/pushing it.
	 * Logging/reporting IS supported here whenever \e log != null.
	 */
	public static
	ImgPlus<?> receiveImage(final int portNo,
	                        final int timeOut, final ProgressCallback log)
	throws IOException
	{
		final ZeroMQInputStream zis = new ZeroMQInputStream(portNo, timeOut);

		final ImgStreamer is = new ImgStreamer(log);
		final ImgPlus<?> imgP = is.read(zis);

		zis.close();
		return imgP;
	}

	/**
	 * Receives an image over network from someone who is sending/pushing it.
	 * No logging/reporting is supported here.
	 */
	public static
	ImgPlus<?> receiveImage(final int portNo,
	                        final int timeOut)
	throws IOException
	{ return receiveImage(portNo, timeOut, null); }

	/**
	 * Receives an image over network from someone who is sending/pushing it.
	 * No logging/reporting is supported here, default 30 secs timeout is used.
	 */
	public static
	ImgPlus<?> receiveImage(final int portNo)
	throws IOException
	{ return receiveImage(portNo, 30, null); }


	// ------------------ static, single-image handling functions ------------------
	// ------------------ serve + request vatiants ------------------

	/**
	 * Serves an image over network to someone who is receiving/pulling it,
	 * it acts in fact as the sendImage() but connection is initiated from
	 * the receiver (the other peer).
	 * Logging/reporting IS supported here whenever \e log != null.
	 */
	public static <T extends NativeType<T>>
	void serveImage(final ImgPlus<T> imgP, final int portNo,
	                final int timeOut, final ProgressCallback log)
	throws IOException
	{
		final ZeroMQOutputStream zos = new ZeroMQOutputStream(portNo, timeOut);

		final ImgStreamer is = new ImgStreamer(log);
		is.setImageForStreaming(imgP);
		is.write(zos);

		zos.close();
	}

	/**
	 * Serves an image over network to someone who is receiving/pulling it,
	 * it acts in fact as the sendImage() but connection is initiated from
	 * the receiver (the other peer).
	 * No logging/reporting is supported here.
	 */
	public static <T extends NativeType<T>>
	void serveImage(final ImgPlus<T> imgP, final int portNo,
	                final int timeOut)
	throws IOException
	{ serveImage(imgP, portNo, timeOut, null); }

	/**
	 * Serves an image over network to someone who is receiving/pulling it,
	 * it acts in fact as the sendImage() but connection is initiated from
	 * the receiver (the other peer).
	 * No logging/reporting is supported here, default 30 secs timeout is used.
	 */
	public static <T extends NativeType<T>>
	void serveImage(final ImgPlus<T> imgP, final int portNo)
	throws IOException
	{ serveImage(imgP, portNo, 30, null); }


	/**
	 * Receives/pulls an image over network from someone who is serving it,
	 * it acts in fact as the receiveImage() but connection is initiated from
	 * this function (the receiver).
	 * Logging/reporting IS supported here whenever \e log != null.
	 */
	public static
	ImgPlus<?> requestImage(final String addr,
	                        final int timeOut, final ProgressCallback log)
	throws IOException
	{
		final ZeroMQInputStream zis = new ZeroMQInputStream("tcp://"+addr, timeOut);

		final ImgStreamer is = new ImgStreamer(log);
		final ImgPlus<?> imgP = is.read(zis);

		zis.close();
		return imgP;
	}

	/**
	 * Receives/pulls an image over network from someone who is serving it,
	 * it acts in fact as the receiveImage() but connection is initiated from
	 * this function (the receiver).
	 * No logging/reporting is supported here.
	 */
	public static
	ImgPlus<?> requestImage(final String addr,
	                        final int timeOut)
	throws IOException
	{ return requestImage(addr, timeOut, null); }

	/**
	 * Receives/pulls an image over network from someone who is serving it,
	 * it acts in fact as the receiveImage() but connection is initiated from
	 * this function (the receiver).
	 * No logging/reporting is supported here, default 30 secs timeout is used.
	 */
	public static
	ImgPlus<?> requestImage(final String addr)
	throws IOException
	{ return requestImage(addr, 30, null); }
}
