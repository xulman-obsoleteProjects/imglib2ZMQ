/*
 * To the extent possible under law, the ImageJ developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
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

	/** Sends the content of the buffer as a solo ZMQ message.
	 *
	 * This method should never block longer than this.waitTimeOut.
	 * If the timeout occurs, exception is raised to notify the caller.
	 *
	 * @throws IOException If ZMQ.send() will have some trouble, or if
	 * no confirmation arrives within the waitTimeOut.
	 */
	public
	void writeZMQ(byte[] buf, int pos)
	throws IOException
	{
		//send the data
		zmqSocket.send(buf,0,pos,0);
		//
		//beware! ZMQ does not make a copy of the 'buf' and if it does not send it
		//right away, the future content of the 'buf' will be sent instead of
		//the current content (when the connection is eventually established
		//and if the same 'buf' is re-used)...
		//
		//so, we wait for the confirmation (that assures us the 'buf' has really
		//been transmitted over), or complain

		//wait until we hear back from the recipient,
		//or until we timeout
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
			final byte[] confMsg = zmqSocket.recv();

			if (confMsg[0] != 'O')
				throw new IOException("wrong confirmation detected, communication is broken");
		}
		else
			throw new IOException("no confirmation detected even after "+waitTimeOut+" seconds");
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

			zmqSocket.send(confirmationMsg);
			return buf;
		}

		return zeroLengthByteArray;
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

	private static final byte[] zeroLengthByteArray = new byte[0];
	private static final byte[] confirmationMsg = new byte[] { 'O','K' };
}
