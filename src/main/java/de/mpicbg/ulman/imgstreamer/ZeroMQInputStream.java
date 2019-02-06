package de.mpicbg.ulman.imgstreamer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.IOException;
import java.io.InputStream;

public class ZeroMQInputStream extends InputStream
{
	// -------------- buffering stuff --------------
	private final byte[] buf = new byte[1024000];
	private int pos = 0;
	private int lim = 0;

	/** Reads another batch of data.
	 *
	 * It is assumed that the caller knows how much data to expect,
	 * and that she would not ask for more. That is because when the
	 * internal buffer is empty, the method will poll (a couple of
	 * times until a waitTimeOut is over) the ZMQ message queue to
	 * fill the internal buffer. If timeout occurs, the method returns
	 * with -1, which is a value signalling the end of the stream.
	 * The method cannot, however, recognize that it needs to wait
	 * longer because the network stream is possibly not over yet --
	 * this is why the user has to setup sufficiently large waitTimeOut.
	 *
	 * @return The next available byte of the ZMQ message.
	 * @throws IOException If ZMQ.recv() will have some trouble.
	 */
	@Override
	public int read()
	throws IOException
	{
		if (pos == lim)
		{
			int waitTime = 0;
			while (!isZMQready() && waitTime < waitTimeOut)
			{
				//we wait 1 second before another read (aka buffer filling) attempt
				waitTime += 1;
				try {
					Thread.sleep(1000);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			if (isZMQready()) readZMQ();
		}

		if (pos < lim) return buf[pos++];
		else return -1;
	}

	public int available()
	throws IOException
	{
		return lim-pos;
	}

	// -------------- ZMQ stuff --------------
	/** period of time (in seconds) to wait for next ZMQ message */
	public int waitTimeOut = 120;

	//init the communication side
	//ZMQ.Context zmqContext = ZMQ.context(1);
	//ZMQ.Socket writerSocket = null;

	/** reads the ZMQ message into our own buffer, but only if
	 *  there is enough capacity to store the incoming message */
	private
	void readZMQ()
	{
		//add 10 more fake values into the buffer
		int p=lim;
		for (; p < lim+10 && p < 40; ++p) buf[p] = (byte)p;
		lim = p;
	}

	/** non-blocking pooling of ZMQ message queue,
	 *  returns true if some message is available */
	private
	boolean isZMQready()
	{
		if (lim < 40)
			return true;
		else
			return false;
	}
}
