package de.mpicbg.ulman.imgstreamer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

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
		if (pos == buf.length) readZMQ();

		if (pos < buf.length) return buf[pos++] & 0xFF;
		else return -1;
	}

	@Override
	public int available()
	{
		return buf.length - pos;
	}

	/** inits this InputStream by binding to a local port */
	public
	ZeroMQInputStream(final int portNo)
	throws IOException
	{
		initSocketWithBind(portNo);
	}

	/** inits this InputStream by binding to a local port */
	public
	ZeroMQInputStream(final int portNo, final int timeOut)
	throws IOException
	{
		waitTimeOut = timeOut;
		initSocketWithBind(portNo);
	}

	/** inits this InputStream by connecting to given URL */
	public
	ZeroMQInputStream(final String URL)
	throws IOException
	{
		initSocketWithConnect(URL);
	}

	/** inits this InputStream by connecting to given URL */
	public
	ZeroMQInputStream(final String URL, final int timeOut)
	throws IOException
	{
		waitTimeOut = timeOut;
		initSocketWithConnect(URL);
	}

	// -------------- ZMQ stuff --------------
	/** period of time (in seconds) to wait for next ZMQ message */
	public int waitTimeOut = 120;

	//init the communication side
	ZMQ.Context zmqContext = ZMQ.context(1);
	ZMQ.Socket zmqSocket = null;

	private
	void initSocketWithBind(final int portNo)
	throws IOException
	{
		try {
			zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.bind("tcp://*:" + portNo);
		}
		catch (ZMQException e) {
			throw new IOException("network error: " + e.getMessage());
		}
		catch (Exception e) {
			throw new IOException("other error: " + e.getMessage());
		}
	}

	private
	void initSocketWithConnect(final String URL)
	throws IOException
	{
		try {
			zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.connect(URL);
		}
		catch (ZMQException e) {
			throw new IOException("network error: " + e.getMessage());
		}
		catch (Exception e) {
			throw new IOException("other error: " + e.getMessage());
		}
	}

	/** Reads the ZMQ message into the buffer, assuming that the content
	 *  of the current buffer has been completely read out (because this
	 *  method will "overwrite" it).
	 *
	 * This method should never block longer than this.waitTimeOut.
	 * If the timeout occurs, the method silently quits without any
	 * change to the underlying buffer.
	 *
	 * @throws IOException If ZMQ.recv() will have some trouble.
	 */
	private
	void readZMQ()
	throws IOException
	{
		int waitTime = 0;
		while (!isZMQready() && waitTime < waitTimeOut)
		{
			//we wait 1 second
			waitTime += 1;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
		}

		if (isZMQready())
		{
			buf = zmqSocket.recv();
			if (buf == null)
				 throw new IOException("network reading error");
			pos = 0;
		}
	}

	/** non-blocking pooling of ZMQ message queue,
	 *  returns true if some message is available */
	private
	boolean isZMQready()
	{
		return ( (zmqSocket.getEvents() & ZMQ.EVENT_CONNECTED) == ZMQ.EVENT_CONNECTED );
	}

	/** request to close the socket */
	@Override
	public
	void close()
	{
		zmqSocket.close();
	}
}
