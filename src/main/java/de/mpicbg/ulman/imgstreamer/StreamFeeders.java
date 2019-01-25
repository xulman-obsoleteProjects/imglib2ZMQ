/*
 * To the extent possible under law, the ImageJ developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
package de.mpicbg.ulman.imgstreamer;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;

public class StreamFeeders
{
	public static
	interface StreamFeeder
	{
		void write(final Object inArray, final DataOutputStream outStream) throws IOException;
		void  read(final DataInputStream inStream, final Object outArray) throws IOException;
	}

	public static
	class ByteStreamFeeder implements StreamFeeder
	{
		@Override
		public void write(final Object inArray, final DataOutputStream outStream) throws IOException
		{
			outStream.write((byte[])inArray);
		}
		@Override
		public void read(final DataInputStream inStream, final Object outArray) throws IOException
		{
			inStream.read((byte[])outArray);
		}
	}

	public static
	class ShortStreamFeeder implements StreamFeeder
	{
		@Override
		public void write(final Object inArray, final DataOutputStream outStream) throws IOException
		{
			final short[] inShorts = (short[])inArray;
			for (int i=0; i < inShorts.length; ++i) outStream.writeShort(inShorts[i]);
		}
		@Override
		public void read(final DataInputStream inStream, final Object outArray) throws IOException
		{
			final short[] outShorts = (short[])outArray;
			for (int i=0; i < outShorts.length; ++i) outShorts[i]=inStream.readShort();
		}
	}

	public static
	class FloatStreamFeeder implements StreamFeeder
	{
		@Override
		public void write(final Object inArray, final DataOutputStream outStream) throws IOException
		{
			final float[] inFloats = (float[])inArray;
			for (int i=0; i < inFloats.length; ++i) outStream.writeFloat(inFloats[i]);
		}
		@Override
		public void read(final DataInputStream inStream, final Object outArray) throws IOException
		{
			final float[] outFloats = (float[])outArray;
			for (int i=0; i < outFloats.length; ++i) outFloats[i]=inStream.readFloat();
		}
	}

	public static
	class DoubleStreamFeeder implements StreamFeeder
	{
		@Override
		public void write(final Object inArray, final DataOutputStream outStream) throws IOException
		{
			final double[] inDoubles = (double[])inArray;
			for (int i=0; i < inDoubles.length; ++i) outStream.writeDouble(inDoubles[i]);
		}
		@Override
		public void read(final DataInputStream inStream, final Object outArray) throws IOException
		{
			final double[] outDoubles = (double[])outArray;
			for (int i=0; i < outDoubles.length; ++i) outDoubles[i]=inStream.readDouble();
		}
	}

	public static
	StreamFeeder createStreamFeeder(final Object sampleArray)
	{
		if (sampleArray instanceof byte[])
		{
			return new ByteStreamFeeder();
		}
		else
		if (sampleArray instanceof short[])
		{
			return new ShortStreamFeeder();
		}
		else
		if (sampleArray instanceof float[])
		{
			return new FloatStreamFeeder();
		}
		else
		if (sampleArray instanceof double[])
		{
			return new DoubleStreamFeeder();
		}
		else
			throw new RuntimeException("Unsupported voxel storage, sorry.");
	}
}
