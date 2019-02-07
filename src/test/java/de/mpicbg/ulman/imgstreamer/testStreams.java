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
	public static void main(String... args)
	{
		//testQueingOfObjectsInAStream();

		for(Class<? extends NativeType> aClass : ImgStreamer.SUPPORTED_VOXEL_CLASSES)
		{
			try {
				NativeType<?> type = aClass.newInstance();

				testImgPlus( fillImg(
						new ArrayImgFactory(type).create(200,100,5)
				));

				testImgPlus( fillImg(
						new PlanarImgFactory(type).create(200,100,5)
				));

				testImgPlus( fillImg(
						new CellImgFactory(type, 50,20,5).create(200,100,5)
				));
			} catch (InstantiationException | IllegalAccessException e) {
				throw new RuntimeException(e);
			}
		}
	}


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

	static <T extends RealType<T> & NativeType<T>>
	void testImgPlus(final Img<T> img)
	{
		try {
			//create ImgPlus out of the input Img
			ImgPlus<T> imgP = new ImgPlus<>(img);

			//sample output stream
			final OutputStream os = new FileOutputStream("/tmp/out.dat");
			//
			//ImgStreamer must always be instantiated if you want to know the complete
			//stream size before the actual serialization into an OutputStream
			//ImgStreamer isv = new ImgStreamer( new myLogger() );
			ImgStreamer isv = new ImgStreamer( null );
			isv.setImageForStreaming(imgP);
			System.out.println("stream length will be: "+isv.getOutputStreamLength());
			isv.write(os);
			//
			os.close();

			//sample input stream
			final InputStream is = new FileInputStream("/tmp/out.dat");
			//
			ImgPlus<? extends RealType<?>> imgPP = isv.readAsRealTypedImg(is);
			//
			is.close();
			System.out.println("got this image: "+imgPP.getImg().toString()
			                  +" of "+imgPP.getImg().firstElement().getClass().getSimpleName());

			Cursor<T> cP = imgP.getImg().cursor();
			cP.jumpFwd(50);

			Cursor<? extends RealType<?>> cPP = imgPP.getImg().cursor();
			cPP.jumpFwd(50);

			if (cP.get().getRealDouble() != cPP.get().getRealDouble())
				System.out.println("----------> PIXEL VALUES MISMATCH! <----------");

		} catch (IOException e) {
			e.printStackTrace();
		}
	}


	static
	void testQueingOfObjectsInAStream()
	{
		try {
			final ObjectOutputStream os = new ObjectOutputStream( new FileOutputStream("/tmp/out.dat") );
			os.writeUTF("ahoj");
			os.writeUTF("clovece");
			os.close();

			final ObjectInputStream is = new ObjectInputStream( new FileInputStream("/tmp/out.dat") );
			System.out.println(is.readUTF());
			System.out.println(is.readUTF());
			is.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
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
