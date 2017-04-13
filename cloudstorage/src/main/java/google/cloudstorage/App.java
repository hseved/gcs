package google.cloudstorage;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.nio.channels.WritableByteChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Iterator;
import java.util.zip.Checksum;

import com.google.cloud.Page;
import com.google.cloud.ReadChannel;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobListOption;
import com.google.cloud.storage.StorageOptions;
import com.google.common.io.BaseEncoding;
import com.google.common.primitives.Ints;


/**
 * Hello world!
 *
 */
public class App {
	public static void run(Storage storage, BlobId blobId, Path downloadTo) throws IOException {
		Blob blob = storage.get(blobId);
		if (blob == null) {
			System.out.println("No such object");
			return;
		}
		PrintStream writeTo = System.out;
		if (downloadTo != null) {
			writeTo = new PrintStream(new FileOutputStream(downloadTo.toFile()));
		}
		if (blob.getSize() < 10000) {
			// Blob is small read all its content in one request
			byte[] content = blob.getContent();
			writeTo.write(content);
		} else {
			System.out.println(blob.getCrc32c());
			// When Blob size is big or unknown use the blob's channel reader.
			try (ReadChannel reader = blob.reader()) {
				WritableByteChannel channel = Channels.newChannel(writeTo);
				ByteBuffer bytes = ByteBuffer.allocate(64 * 1024);
				while (reader.read(bytes) > 0) {
					bytes.flip();
					channel.write(bytes);
					bytes.clear();
				}
			}
		}
		if (downloadTo == null) {
			writeTo.println();
		} else {
			writeTo.close();
		}
	}
	
	
	public static long getCRC32(InputStream in) throws IOException {
	    Checksum sum_control = new Crc32c();
	    for (int b = in.read(); b != -1; b = in.read()) {
	      sum_control.update(b);
	    }
	    return sum_control.getValue();
	    
	  }
	
	public static void main(String... args) throws Exception {

		Storage storage =StorageOptions.getDefaultInstance().getService();
		// Instantiates a client
		//		Storage storage = StorageOptions.newBuilder()
		//			    .setCredentials(ServiceAccountCredentials.fromStream(new FileInputStream("/path/to/my/key.json")))
		//			    .build()
		//			    .getService();


		// The name for the new bucket
		String bucketName = "devesh_test";  // "my-new-bucket";

		Page<Blob> listBuckets = storage.list(bucketName,BlobListOption.currentDirectory());
		Iterator<Blob> blobIterator = listBuckets.iterateAll();
		while (blobIterator.hasNext()) {
			Blob blob = blobIterator.next();
			System.out.println(blob.getMetadata());
		}
		
		String arg2="/Users/devesh.sriniva/Desktop";
		String arg1="dcm_account6092_click_2017030300_20170307_055716_533361896.csv.gz";
		Path path;
		path = Paths.get(arg2);
		if (Files.isDirectory(path)) {
			path = path.resolve(Paths.get(arg1).getFileName());
		}
		
		//run(storage, BlobId.of(bucketName, arg1), path);
		long crcValue = getCRC32(new FileInputStream(path.toFile()));
		
		  //byte[] bArray = String.valueOf(Arrays.copyOfRange(Longs.toByteArray(crcValue), 4, 8)).getBytes();
		  
		  byte[] crcBytes = Ints.toByteArray((int) crcValue);
		  System.out.println(BaseEncoding.base64().encode(crcBytes));
		

	}

}
