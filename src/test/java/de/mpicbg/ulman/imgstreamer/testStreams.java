package de.mpicbg.ulman.imgstreamer;

import java.io.*;

import net.imagej.ImgPlus;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.planar.PlanarImgFactory;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;

public class testStreams
{
	static class myLogger implements ProgressCallback
	{
		@Override
		public void info(String msg) {
			System.out.println(msg);
		}
		@Override
		public void setProgress(float howFar) {
			System.out.println("progress: "+howFar);
		}
	}

	static <T extends RealType<T>, U extends RealType<U>>
	boolean areBothImagesTheSame(final ImgPlus<T> imgA, final ImgPlus<U> imgB)
	{
		Cursor<T> cA = imgA.getImg().cursor();
		cA.jumpFwd(50);

		Cursor<U> cB = imgB.getImg().cursor();
		cB.jumpFwd(50);

		if (cA.get().getRealDouble() != cB.get().getRealDouble())
		{
			System.out.println("----------> PIXEL VALUES MISMATCH! <----------");
			return false;
		}

		//one test more never hurts...
		cA.jumpFwd(50);
		cB.jumpFwd(50);
		if (cA.get().getRealDouble() != cB.get().getRealDouble())
		{
			System.out.println("----------> PIXEL VALUES MISMATCH! <----------");
			return false;
		}

		return true;
	}

	static <T extends RealType<T>>
	Img<T> fillImg(final Img<T> img)
	{
		long counter = 0;

		Cursor<T> imgC = img.cursor();
		while (imgC.hasNext())
			imgC.next().setReal(counter++);

		return img;
	}
}
