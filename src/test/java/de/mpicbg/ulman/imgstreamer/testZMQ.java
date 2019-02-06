package de.mpicbg.ulman.imgstreamer;

import java.io.*;


public class testZMQ
{
	public static void main(String... args)
	{
		try {

			final ZeroMQInputStream zis = new ZeroMQInputStream();
			int nextVal = 0;
			do {
				nextVal = zis.read();
				System.out.println("got now this value: "+nextVal);
			} while (nextVal > -1);

		} catch (IOException e) {
			e.printStackTrace();
		}
	}
}
