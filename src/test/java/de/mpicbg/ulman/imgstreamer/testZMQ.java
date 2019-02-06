package de.mpicbg.ulman.imgstreamer;

import java.io.*;
import org.zeromq.ZMQ;
import org.zeromq.ZMQException;

public class testZMQ
{
	public static void main(String... args)
	{
		try {
			ZMQ.Context zmqContext = ZMQ.context(1);
			ZMQ.Socket zmqSocket = zmqContext.socket(ZMQ.PAIR);
			zmqSocket.connect("tcp://localhost:3456");

			byte[] testSendArray = new byte[] { 97,98,99,100,97,98,99,100,97,98,99,127 };
			zmqSocket.send(testSendArray);

			final ZeroMQInputStream zis = new ZeroMQInputStream(3456, 5);
			int nextVal = 0;
			do {
				nextVal = zis.read();
				System.out.println("got now this value: "+nextVal);

				if (nextVal == 127)
				{
					//modify the buffer to recognize it...
					for (int i=0; i < testSendArray.length; ++i) testSendArray[i]+=4;
					zmqSocket.send(testSendArray);
				}
			} while (nextVal > -1);

			zmqSocket.close();

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
