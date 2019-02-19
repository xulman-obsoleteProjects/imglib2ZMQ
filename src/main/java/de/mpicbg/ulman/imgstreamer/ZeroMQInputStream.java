/*
 * To the extent possible under law, the ImageJ developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
package de.mpicbg.ulman.imgstreamer;

import java.io.IOException;
import java.io.InputStream;

public class ZeroMQInputStream extends InputStream
{
	// -------------- buffering stuff --------------
	private byte[] buf = new byte[0]; //initially an empty buffer
	private int pos = 0;

	/** Reads one (1) byte from the underlying buffer, which is populated
	 *  from the underlying ZMQ communication.
	 *
	 * @return The next available byte of the ZMQ message, or -1 if none available.
	 * @throws IOException If ZMQ.recv() will have some trouble.
	 */
	@Override
	public int read()
	throws IOException
	{
		if (pos == buf.length)
		{
			buf = zmq.readZMQ();
			pos = 0;
		}

		if (pos < buf.length) return buf[pos++] & 0xFF;
		else return -1;
	}

	@Override
	public int available()
	{
		return buf.length - pos;
	}

	/** request to close the stream */
	@Override
	public void close()
	{
		zmq.close();
	}

	// -------------- ZMQ stuff --------------
	private final ZeroMQsession zmq;

	/** inits this InputStream by binding to a local port */
	public
	ZeroMQInputStream(final int portNo)
	throws IOException
	{
		zmq = new ZeroMQsession(portNo);
	}

	/** inits this InputStream by binding to a local port */
	public
	ZeroMQInputStream(final int portNo, final int timeOut)
	throws IOException
	{
		zmq = new ZeroMQsession(portNo,timeOut);
	}

	/** inits this InputStream by connecting to given URL */
	public
	ZeroMQInputStream(final String URL)
	throws IOException
	{
		zmq = new ZeroMQsession(URL);
	}

	/** inits this InputStream by connecting to given URL */
	public
	ZeroMQInputStream(final String URL, final int timeOut)
	throws IOException
	{
		zmq = new ZeroMQsession(URL,timeOut);
	}
}
