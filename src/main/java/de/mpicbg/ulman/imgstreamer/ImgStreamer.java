/*
 * To the extent possible under law, the ImageJ developers have waived
 * all copyright and related or neighboring rights to this tutorial code.
 *
 * See the CC0 1.0 Universal license for details:
 *     http://creativecommons.org/publicdomain/zero/1.0/
 */
package de.mpicbg.ulman.imgstreamer;

import net.imagej.Dataset;
import net.imagej.ImgPlus;
import net.imglib2.Cursor;
import net.imglib2.img.Img;
import net.imglib2.img.AbstractImg;
import net.imglib2.img.WrappedImg;
import net.imglib2.img.array.ArrayImg;
import net.imglib2.img.array.ArrayImgFactory;
import net.imglib2.img.cell.AbstractCellImg;
import net.imglib2.img.cell.Cell;
import net.imglib2.img.cell.CellImg;
import net.imglib2.img.cell.CellImgFactory;
import net.imglib2.img.planar.PlanarImg;
import net.imglib2.img.planar.PlanarImgFactory;
import net.imglib2.img.basictypeaccess.array.ArrayDataAccess;
import net.imglib2.type.NativeType;
import net.imglib2.type.numeric.RealType;
import net.imglib2.type.numeric.integer.ByteType;
import net.imglib2.type.numeric.integer.UnsignedByteType;
import net.imglib2.type.numeric.integer.ShortType;
import net.imglib2.type.numeric.integer.UnsignedShortType;
import net.imglib2.type.numeric.real.FloatType;
import net.imglib2.type.numeric.real.DoubleType;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.OutputStream;
import java.io.InputStream;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;
import java.util.StringTokenizer;

import static de.mpicbg.ulman.imgstreamer.StreamFeeders.createStreamFeeder;

public class ImgStreamer
{
	/// list of supported voxel types: so far only scalar images are supported
	static public final List<Class<? extends NativeType>> SUPPORTED_VOXEL_CLASSES
		= Arrays.asList(ByteType.class, UnsignedByteType.class, ShortType.class,
		                UnsignedShortType.class, FloatType.class, DoubleType.class);

	/// default logger that logs nowhere
	static public final
	class EmptyProgressCallback implements ProgressCallback
	{
		@Override
		public void info(String msg) {}

		@Override
		public void setProgress(float howFar) {}
	}

	/// currently used logger
	private
	final ProgressCallback logger;

	public
	ImgStreamer(final ProgressCallback _log)
	{
		logger = _log == null? new EmptyProgressCallback(): _log;
	}


	// -------- streaming stuff OUT --------
	//reference on the image to be streamed
	private Img<? extends NativeType<?>> img;

	//header and metadata (from ImgPlus) corresponding to the image
	private String headerMsg;
	private byte[] metadataBytes;
	private long voxelBytesCount;

	public <T extends NativeType<T>, A extends ArrayDataAccess<A>>
	void setImageForStreaming(final ImgPlus<T> imgToBeStreamed)
	{
		img = getUnderlyingImg(imgToBeStreamed);
		if (img.size() == 0)
			throw new RuntimeException("Refusing to stream an empty image...");

		Class<?> voxelClass = img.firstElement().getClass();
		if(!SUPPORTED_VOXEL_CLASSES.contains(voxelClass))
			throw new IllegalArgumentException("Unsupported voxel type, sorry.");

		//build the corresponding header:
		//protocol version
		headerMsg = new String("v1");

		//dimensionality data
		voxelBytesCount = 1;
		headerMsg += " dimNumber " + img.numDimensions();
		for (int i=0; i < img.numDimensions(); ++i)
		{
			headerMsg += " " + img.dimension(i);
			voxelBytesCount *= img.dimension(i);
		}

		//decipher the voxel type
		headerMsg += " " + voxelClass.getSimpleName();

		//check we can handle the storage model of this image,
		//and check the pixel size
		Object sampleArray = null;
		if (img instanceof ArrayImg)
		{
			headerMsg += " ArrayImg ";
			sampleArray = ((ArrayImg<?,A>)img).update(null).getCurrentStorageArray();
		}
		else
		if (img instanceof PlanarImg)
		{
			headerMsg += " PlanarImg ";
			sampleArray = ((PlanarImg<?,A>)img).getPlane(0).getCurrentStorageArray();
		}
		else
		//if (img instanceof CellImg || img instanceof LazyCellImg)
		if (img instanceof AbstractCellImg)
		{
			//DiskCachedCellImg<> and SCIFIOCellImg<> extends (besides other) LazyCellImg<>
			//LazyCellImg<> extends AbstractCellImg<T, A, Cell<A>, LazyCellImg.LazyCells<Cell<A>>>
			//  CellImg<>   extends AbstractCellImg<T, A, Cell<A>, ListImg<Cell<A>>>
			//both ListImg<C> and LazyCellImg.LazyCells<C> extends AbstractImg<C>
			final AbstractCellImg<?,A, Cell<A>, ? extends AbstractImg<Cell<A>>> cellImg
			   = (AbstractCellImg<?,A, Cell<A>, ? extends AbstractImg<Cell<A>>>)img;

			//export also the internal configuration of tiles (that make up this image)
			headerMsg += " CellImg ";
			for (int i=0; i < cellImg.numDimensions(); ++i)
				headerMsg += cellImg.getCellGrid().cellDimension(i) + " ";

			sampleArray = cellImg.getCells().firstElement().getData().getCurrentStorageArray();
		}
		else
			throw new RuntimeException("Cannot determine the type of image, cannot stream it.");

		long pixelSize;
		if (sampleArray instanceof byte[])
		{
			pixelSize = 1;
		}
		else
		if (sampleArray instanceof short[])
		{
			pixelSize = 2;
		}
		else
		if (sampleArray instanceof float[])
		{
			pixelSize = 4;
		}
		else
		if (sampleArray instanceof double[])
		{
			pixelSize = 8;
		}
		else
			throw new RuntimeException("Unsupported voxel storage, sorry.");
		voxelBytesCount *= pixelSize;

		//process the metadata....
		metadataBytes = packAndSendPlusData(imgToBeStreamed);
	}


	public
	long getOutputStreamLength()
	{
		return headerMsg.length()+metadataBytes.length+4 + voxelBytesCount;
	}


	public <A extends ArrayDataAccess<A>>
	void write(final OutputStream os)
	throws IOException
	{
		final DataOutputStream dos = new DataOutputStream(os);

		logger.info("streaming the header: "+headerMsg);
		dos.writeUTF(headerMsg);
		if (dos.size() != headerMsg.length()+2)
		{
			//System.out.println("dos.size()="+dos.size());
			throw new RuntimeException("Header size calculation mismatch.");
		}

		logger.info("streaming the metadata...");
		dos.writeShort(metadataBytes.length);
		dos.write(metadataBytes);
		if (dos.size() != headerMsg.length()+metadataBytes.length+4)
		{
			//System.out.println("dos.size()="+dos.size());
			throw new RuntimeException("Metadata size calculation mismatch.");
		}

		logger.info("streaming the image data...");
		if (img instanceof ArrayImg)
		{
			packAndSendArrayImg((ArrayImg)img,
				createStreamFeeder( ((ArrayImg<?,A>)img).update(null).getCurrentStorageArray() ),
				dos);
		}
		else
		if (img instanceof PlanarImg)
		{
			packAndSendPlanarImg((PlanarImg)img,
				createStreamFeeder( ((PlanarImg<?,A>)img).getPlane(0).getCurrentStorageArray() ),
				dos);
		}
		else
		if (img instanceof AbstractCellImg)
		{
			final AbstractCellImg<?,A, Cell<A>, ? extends AbstractImg<Cell<A>>> cellImg
			   = (AbstractCellImg<?,A, Cell<A>, ? extends AbstractImg<Cell<A>>>)img;

			packAndSendCellImg(cellImg,
				createStreamFeeder( cellImg.getCells().firstElement().getData().getCurrentStorageArray() ),
				dos);
		}
		else
			throw new RuntimeException("Unsupported image backend type, sorry.");

		dos.flush();
		logger.info("streaming finished.");
	}


	// -------- streaming stuff IN --------
	public
	ImgPlus<? extends RealType<?>> readAsRealTypedImg(final InputStream is)
	throws IOException
	{
		// this is a hack but we can afford it because the this.SUPPORTED_VOXEL_CLASSES
		// contains only RealType-extensible voxel types
		return (ImgPlus)read(is);
	}

	public
	ImgPlus<?> read(final InputStream is)
	throws IOException
	{
		final DataInputStream dis = new DataInputStream(is);

	    //read the header
		final String header = dis.readUTF();

		//read the metadata
		final byte[] metadata = new byte[dis.readShort()];
		dis.read(metadata);

		//process the header
		logger.info("found header: "+header);
		StringTokenizer headerST = new StringTokenizer(header, " ");
		if (! headerST.nextToken().startsWith("v1"))
			throw new RuntimeException("Unknown protocol, expecting protocol v1.");

		if (! headerST.nextToken().startsWith("dimNumber"))
			throw new RuntimeException("Incorrect protocol, expecting dimNumber.");
		final int n = Integer.valueOf(headerST.nextToken());

		//fill the dimensionality data
		final int[] dims = new int[n];
		for (int i=0; i < n; ++i)
			dims[i] = Integer.valueOf(headerST.nextToken());

		final String typeStr = headerST.nextToken();
		final String backendStr = headerST.nextToken();

		//if CellImg is stored in the stream, unveil also its tile (cells) configuration
		int[] cellDims = null;
		if (backendStr.startsWith("CellImg"))
		{
			//parse out also the cellDims configuration
			cellDims = new int[n];
			for (int i=0; i < n; ++i)
				cellDims[i] = Integer.valueOf(headerST.nextToken());
		}

		//envelope/header message is (mostly) parsed,
		//start creating the output image of the appropriate type
		Img<? extends NativeType<?>> img = createImg(dims, backendStr, createVoxelType(typeStr), cellDims);

		if (img == null)
			throw new RuntimeException("Unsupported image backend type, sorry.");

		//the core Img is prepared, lets extend it with metadata and fill with voxel values afterwards
		//create the ImgPlus from it -- there is fortunately no deep coping
		ImgPlus<?> imgP = new ImgPlus<>(img);

		//process the metadata
		logger.info("processing the incoming metadata...");
		receiveAndUnpackPlusData(metadata,imgP);

		if (img.size() == 0)
			throw new RuntimeException("Refusing to stream an empty image...");

		logger.info("processing the incoming image data...");
		if (img instanceof ArrayImg)
		{
			receiveAndUnpackArrayImg((ArrayImg)img,
				createStreamFeeder( ((ArrayImg<?,? extends ArrayDataAccess<?>>)img).update(null).getCurrentStorageArray() ),
				dis);
		}
		else
		if (img instanceof PlanarImg)
		{
			receiveAndUnpackPlanarImg((PlanarImg)img,
				createStreamFeeder( ((PlanarImg<?,? extends ArrayDataAccess<?>>)img).getPlane(0).getCurrentStorageArray() ),
				dis);
		}
		else
		if (img instanceof CellImg)
		{
			receiveAndUnpackCellImg((CellImg)img,
				createStreamFeeder( ((CellImg<?,? extends ArrayDataAccess<?>>)img).getCells().firstElement().getData().getCurrentStorageArray() ),
				dis);
		}
		else
			throw new RuntimeException("Unsupported image backend type, sorry.");

		logger.info("processing finished.");
		return imgP;
	}


	// -------- support for the transmission of the image metadata --------
	protected
	byte[] packAndSendPlusData(final ImgPlus<?> img)
	{
		//process (only some) metadata from the img
		final byte[] md_name = img.getName() != null ? img.getName().getBytes() : null;
		final int    md_name_length = md_name != null ? md_name.length : 0;

		final byte[] md_source = img.getSource() != null ? img.getSource().getBytes() : null;
		final int    md_source_length = md_source != null ? md_source.length : 0;

		final int    md_validBits = img.getValidBits();

		//final output buffer with "encoded" metadata:
		//NB: might not work out if both 'name' and 'source' parts are veeery long
		final byte[] metadata = new byte[ 4+md_name_length + 4+md_source_length + 4 ];
		int offset=0;

		//part 'name'
		intToByteArray(md_name_length, metadata,offset);
		offset+=4;
		for (int i=0; i < md_name_length; ++i)
			metadata[offset++] = md_name[i];

		//part 'source'
		intToByteArray(md_source_length, metadata,offset);
		offset+=4;
		for (int i=0; i < md_source_length; ++i)
			metadata[offset++] = md_source[i];

		//part 'validBits'
		intToByteArray(md_validBits, metadata,offset);

		return metadata;
	}

	protected
	void receiveAndUnpackPlusData(final byte[] metadata, final ImgPlus<?> img)
	{
		//set the metadata into img
		int offset=0;

		//part 'name'
		final byte[] md_name = new byte[ byteArrayToInt(metadata,offset) ];
		offset+=4;
		for (int i=0; i < md_name.length; ++i)
			 md_name[i] = metadata[offset++];

		//part 'source'
		final byte[] md_source = new byte[ byteArrayToInt(metadata,offset) ];
		offset+=4;
		for (int i=0; i < md_source.length; ++i)
			 md_source[i] = metadata[offset++];

		//part 'validBits'
		final int md_validBits = byteArrayToInt(metadata,offset);

		img.setName(new String(md_name));
		img.setSource(new String(md_source));
		img.setValidBits(md_validBits);
	}

	private void intToByteArray(final int i, final byte[] ba, final int baOffset)
	{
		//MSB format
		ba[baOffset+0] = (byte)((i >> 24) & 0xFF);
		ba[baOffset+1] = (byte)((i >> 16) & 0xFF);
		ba[baOffset+2] = (byte)((i >>  8) & 0xFF);
		ba[baOffset+3] = (byte)(   i      & 0xFF);
	}

	private int byteArrayToInt(final byte[] ba, final int baOffset)
	{
		//MSB format
		int i = ((int)ba[baOffset+0] << 24)
		      | ((int)ba[baOffset+1] << 16)
		      | ((int)ba[baOffset+2] <<  8)
		      |  (int)ba[baOffset+3];

		return i;
	}


	// -------- support for the transmission of the payload/voxel data --------
	protected static <A extends ArrayDataAccess<A>>
	void packAndSendArrayImg(final ArrayImg<?,A> img,
	                         final StreamFeeders.StreamFeeder sf, final DataOutputStream os)
	throws IOException
	{
		sf.write(img.update(null).getCurrentStorageArray(), os);
	}

	protected static <A extends ArrayDataAccess<A>>
	void receiveAndUnpackArrayImg(final ArrayImg<?,A> img,
	                              final StreamFeeders.StreamFeeder sf, final DataInputStream is)
	throws IOException
	{
		sf.read(is, img.update(null).getCurrentStorageArray());
	}


	protected static <A extends ArrayDataAccess<A>>
	void packAndSendPlanarImg(final PlanarImg<?,A> img,
	                          final StreamFeeders.StreamFeeder sf, final DataOutputStream os)
	throws IOException
	{
		for (int slice = 0; slice < img.numSlices(); ++slice)
			sf.write(img.getPlane(slice).getCurrentStorageArray(), os);
	}

	protected static <A extends ArrayDataAccess<A>>
	void receiveAndUnpackPlanarImg(final PlanarImg<?,A> img,
	                               final StreamFeeders.StreamFeeder sf, final DataInputStream is)
	throws IOException
	{
		for (int slice = 0; slice < img.numSlices(); ++slice)
			sf.read(is, img.getPlane(slice).getCurrentStorageArray());
	}


	protected static <A extends ArrayDataAccess<A>>
	void packAndSendCellImg(final AbstractCellImg<?,A, Cell<A>, ? extends AbstractImg<Cell<A>>> img,
	                        final StreamFeeders.StreamFeeder sf, final DataOutputStream os)
	throws IOException
	{
		Cursor<Cell<A>> cell = img.getCells().cursor();
		while (cell.hasNext())
			sf.write(cell.next().getData().getCurrentStorageArray(), os);
	}

	protected static <A extends ArrayDataAccess<A>>
	void receiveAndUnpackCellImg(final AbstractCellImg<?,A, Cell<A>, ? extends AbstractImg<Cell<A>>> img,
	                             final StreamFeeders.StreamFeeder sf, final DataInputStream is)
	throws IOException
	{
		Cursor<Cell<A>> cell = img.getCells().cursor();
		while (cell.hasNext())
			sf.read(is, cell.next().getData().getCurrentStorageArray());
	}


	// -------- the types war --------
	/*
	 * Keeps unwrapping the input image \e img
	 * until it gets to the underlying pure imglib2.Img.
	 */
	@SuppressWarnings("unchecked")
	private static <Q>
	Img<Q> getUnderlyingImg(final Img<Q> img)
	{
		if (img instanceof Dataset)
			return (Img<Q>) getUnderlyingImg( ((Dataset)img).getImgPlus() );
		else if (img instanceof WrappedImg)
			return getUnderlyingImg( ((WrappedImg<Q>)img).getImg() );
		else
			return img;
	}

	@SuppressWarnings("rawtypes") // use raw type because of insufficient support of reflexive types in java
	private static
	NativeType createVoxelType(String typeStr)
	{
		for(Class<? extends NativeType> aClass : SUPPORTED_VOXEL_CLASSES)
			if(typeStr.startsWith(aClass.getSimpleName()))
				try {
					return aClass.newInstance();
				} catch (InstantiationException | IllegalAccessException e) {
					throw new RuntimeException(e);
				}
		throw new IllegalArgumentException("Unsupported voxel type, sorry.");
	}

	private static <T extends NativeType<T>>
	Img<T> createImg(int[] dims, String backendStr, T type, int... cellDims)
	{
		if (backendStr.startsWith("ArrayImg"))
            return new ArrayImgFactory<>(type).create(dims);
		if (backendStr.startsWith("PlanarImg"))
            return new PlanarImgFactory<>(type).create(dims);
		if (backendStr.startsWith("CellImg"))
			return new CellImgFactory<>(type,cellDims).create(dims);
		throw new RuntimeException("Unsupported image backend type, sorry.");
	}
}
