package in.platform.utils.gcs;

import com.google.cloud.ReadChannel;
import com.google.cloud.WriteChannel;
import com.google.cloud.storage.*;
import org.apache.commons.io.IOUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.channels.Channels;
import java.util.Optional;

/**
 * Created by Harsh Verma on 14/08/17.
 */

public class GCSUtils {
    private Storage storage;
    private static final Logger logger = LoggerFactory.getLogger(GCSUtils.class);
    private static final String CACHE_CONTROL_PARAMS = "max-age=3600, public";

    private GCSUtils(Storage storage) {
        this.storage = storage;
    }

    public InputStream getFileFromPath(String googleStoragePath) {
        String[] pathParts = googleStoragePath.split("/");
        String gcsBucketName = pathParts[2];
        String gcsPath = CommonStringUtils.mergeArrayToString(pathParts, "/", 3, pathParts.length);
        return getFile(gcsBucketName, gcsPath);
    }

    public InputStream getFile(String bucketName, String objectName) {
        BlobId blobId = BlobId.of(bucketName, objectName);
        Blob blob = storage.get(blobId);
        ReadChannel readChannel = blob.reader();
        return Channels.newInputStream(readChannel);
    }

    public void uploadObject(String bucket, String path, String filePath) throws Exception {
        File file = new File(filePath);
        this.writeBlob(bucket, path, new FileInputStream(file), file.length(), null, false);
    }


    public void uploadObject(String bucket, String path, String filePath, String contentType) throws Exception {
        File file = new File(filePath);
        this.writeBlob(bucket, path, new FileInputStream(file), file.length(), contentType, false);
    }

    public void uploadObject(String bucket, String path, String filePath, String contentType, boolean enableCacheControl) throws Exception {
        File file = new File(filePath);
        this.writeBlob(bucket, path, new FileInputStream(file), file.length(), contentType, enableCacheControl);
    }

    public void writeBlob(String bucket, String blobName, InputStream inputStream, long contentLength, String contentType, boolean enableCacheControl) throws Exception {
        logger.debug("writeBlob, bucket={}, blobName={}, contentLength={}", bucket, blobName, contentLength);
        BlobId blobId = BlobId.of(bucket, blobName);
        BlobInfo.Builder blobInfoBuilder = BlobInfo.newBuilder(blobId);
        if (enableCacheControl) {
            blobInfoBuilder.setCacheControl(CACHE_CONTROL_PARAMS);
        }
        if (contentType != null) {
            blobInfoBuilder.setContentType(contentType).build();
        }
        BlobInfo blobInfo = blobInfoBuilder.build();
        try (WriteChannel writer = storage.writer(blobInfo)) {
            try {
                writer.write(ByteBuffer.wrap(IOUtils.toByteArray(inputStream), 0, (int) contentLength));
            } catch (Exception ex) {
                logger.error("Exception in writing inputStream to Blob, bucket={}, blob={}, e=", bucket, blobName, ex);
                throw ex;
            }
        } catch (Exception e) {
            logger.error("Error", e);
            throw e;
        }
    }

    public static class Builder {
        private CredentialsProvider credentialsProvider;

        public GCSUtils.Builder withCredentialsProvider(CredentialsProvider credentialsProvider) {
            this.credentialsProvider = credentialsProvider;
            return this;
        }

        public GCSUtils build() {
            CredentialsProvider credentialProvider = Optional.ofNullable(this.credentialsProvider).orElseGet(DefaultCredentialsProvider::new);
            Storage storage = StorageOptions.newBuilder()
                    .setCredentials(credentialProvider.getCredentials())
                    .build().getService();
            return new GCSUtils(storage);
        }
    }
}
