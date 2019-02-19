package de.mpicbg.ulman.imgstreamer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.IOException;
import java.io.OutputStream;

public class ZeroMQOutputStream extends OutputStream
{
	// -------------- buffering stuff --------------
	private final byte[] buf = new byte[1<<20]; //= 1 MB
	private int pos = 0;

	/** Writes one (1) byte into the underlying buffer, which is possibly
	 *  flushed away into the underlying ZMQ communication.
	 *
	 * @throws IOException If ZMQ.send() will have some trouble.
	 */
	@Override
	public void write(int b)
	throws IOException
	{
		if (pos == buf.length) writeZMQ();
		//NB: flush() either returns with pos=0, or with an exception...

		buf[pos++] = (byte)(b & 0xFF); //TODO: is the masking needed?
	}

	@Override
	public void flush()
	throws IOException
	{
		writeZMQ();
	}

	/** inits this OutputStream by binding to a local port */
	public
	ZeroMQOutputStream(final int portNo)
	throws IOException
	{
		initSocketWithBind(portNo);
	}

	/** inits this OutputStream by binding to a local port */
	public
	ZeroMQOutputStream(final int portNo, final int timeOut)
	throws IOException
	{
		waitTimeOut = timeOut;
		initSocketWithBind(portNo);
	}

	/** inits this OutputStream by connecting to given URL */
	public
	ZeroMQOutputStream(final String URL)
	throws IOException
	{
		initSocketWithConnect(URL);
	}

	/** inits this OutputStream by connecting to given URL */
	public
	ZeroMQOutputStream(final String URL, final int timeOut)
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

	/** Sends the current content of the buffer as a solo ZMQ message,
	 *  the buffer is then reset (pos = 0).
	 *
	 * This method should never block longer than this.waitTimeOut.
	 * If the timeout occurs, exception is raised to notify the caller.
	 *
	 * @throws IOException If ZMQ.send() will have some trouble.
	 */
	public
	void writeZMQ()
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

		//beware! ZMQ does not make a copy of the 'buf' and if it does not send it
		//right away, the future content of the 'buf' will be sent instead of
		//the current content (when the connection is eventually established)...
		if (isZMQready())
		{
			zmqSocket.send(buf,0,pos,0);
			pos = 0;
		}
		else
			throw new IOException("no connection detected even after "+waitTimeOut+" seconds");
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
