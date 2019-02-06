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

	/** request to close the socket */
	public
	void close()
	{
		zmqSocket.close();
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

	/** reads the ZMQ message into our own buffer, but only if
	 *  there is enough capacity to store the incoming message */
	private
	void readZMQ()
	throws IOException
	{
		//TODO: if message available is larger than buf.length, increase buf or fail
		lim = zmqSocket.recv(buf,0,buf.length,0);
		if (lim == -1)
		    throw new IOException("network reading error");
		pos = 0;
	}

	/** non-blocking pooling of ZMQ message queue,
	 *  returns true if some message is available */
	private
	boolean isZMQready()
	{
		return ( (zmqSocket.getEvents() & ZMQ.EVENT_CONNECTED) == ZMQ.EVENT_CONNECTED );
	}
}
