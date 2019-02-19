package de.mpicbg.ulman.imgstreamer;

import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

import java.io.IOException;

public class ZeroMQsession
{
	// -------------- API stuff --------------
	/** inits this ZMQ session by binding to a local port */
	public
	ZeroMQsession(final int portNo)
	throws IOException
	{
		initSocketWithBind(portNo);
	}

	/** inits this ZMQ session by binding to a local port */
	public
	ZeroMQsession(final int portNo, final int timeOut)
	throws IOException
	{
		waitTimeOut = timeOut;
		initSocketWithBind(portNo);
	}

	/** inits this ZMQ session by connecting to given URL */
	public
	ZeroMQsession(final String URL)
	throws IOException
	{
		initSocketWithConnect(URL);
	}

	/** inits this ZMQ session by connecting to given URL */
	public
	ZeroMQsession(final String URL, final int timeOut)
	throws IOException
	{
		waitTimeOut = timeOut;
		initSocketWithConnect(URL);
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
	void writeZMQ(byte[] buf, int pos)
	throws IOException
	{
		int waitTime = 0;
		while (!isRecvReady() && waitTime < waitTimeOut)
		{
			//we wait 1 second
			waitTime += 1;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//stop waiting for timeout and move forward...
				waitTime = waitTimeOut;
			}
		}

		//beware! ZMQ does not make a copy of the 'buf' and if it does not send it
		//right away, the future content of the 'buf' will be sent instead of
		//the current content (when the connection is eventually established)...
		if (isRecvReady())
			zmqSocket.send(buf,0,pos,0);
		else
			throw new IOException("no connection detected even after "+waitTimeOut+" seconds");
	}

	/** Reads the ZMQ message into a new buffer.
	 *
	 * This method should never block longer than this.waitTimeOut.
	 * If the timeout occurs, the method quits returning with
	 * zero-length buffer. That said, this method always returns
	 * some buffer.
	 *
	 * @throws IOException If ZMQ.recv() will have some trouble.
	 */
	public
	byte[] readZMQ()
	throws IOException
	{
		int waitTime = 0;
		while (!isRecvReady() && waitTime < waitTimeOut)
		{
			//we wait 1 second
			waitTime += 1;
			try {
				Thread.sleep(1000);
			} catch (InterruptedException e) {
				//stop waiting for timeout and move forward...
				waitTime = waitTimeOut;
			}
		}

		if (isRecvReady())
		{
			byte[] buf = zmqSocket.recv();
			if (buf == null)
				 throw new IOException("network reading error");
			return buf;
		}

		return new byte[0];
	}

	/** request to close the session */
	public
	void close()
	{
		zmqSocket.close();
	}


	// -------------- internal stuff --------------
	/** period of time (in seconds) to wait
	    for the next ZMQ communication */
	protected int waitTimeOut = 120;

	//init the communication side
	private ZMQ.Context zmqContext = ZMQ.context(1);
	private ZMQ.Socket zmqSocket = null;

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

	/** non-blocking pooling of ZMQ message queue,
	 *  returns true if some message is available */
	private
	boolean isRecvReady()
	{
		return ( (zmqSocket.getEvents() & ZMQ.EVENT_CONNECTED) == ZMQ.EVENT_CONNECTED );
	}
}
