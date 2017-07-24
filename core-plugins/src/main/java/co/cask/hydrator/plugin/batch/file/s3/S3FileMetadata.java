/*
 * Copyright © 2017 Cask Data, Inc.
 *  
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *  
 * http://www.apache.org/licenses/LICENSE-2.0
 *  
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package co.cask.hydrator.plugin.batch.file.s3;

import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.hydrator.plugin.batch.file.AbstractFileMetadata;
import org.apache.hadoop.fs.FileStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Filemetadata specific for S3. Defines credentials that are required for
 * connecting to S3.
 */
public class S3FileMetadata extends AbstractFileMetadata {

  public static final String DATA_BASE_NAME = "amazons3";

  private static final Logger LOG = LoggerFactory.getLogger(S3FileMetadata.class);

  public S3FileMetadata(FileStatus fileStatus, String sourcePath, S3Credentials credentials) {
    super(fileStatus, sourcePath, credentials);
  }

  public S3FileMetadata(String fileName, String fileFolder, long timeStamp, String owner, Long fileSize,
                        Boolean isFolder, String baseFolder, short permission, S3Credentials credentials) {
    super(fileName, fileFolder, timeStamp, owner, fileSize, isFolder, baseFolder, permission, credentials);
  }


  public S3FileMetadata(StructuredRecord record) {
    super(record);
  }


  /**
   * S3Credentials. Contains access key, secret key, region, and bucket name.
   */
  public static class S3Credentials extends Credentials {
    public String accessKeyId;
    public String secretKeyId;
    public String region;
    public String bucketName;

    public static final String ACCESS_KEY_ID = "accessKeyId";
    public static final String SECRET_KEY_ID = "secretKeyId";
    public static final String REGION = "region";
    public static final String BUCKET_NAME = "bucketName";

    public static final Schema CREDENTIAL_SCHEMA = Schema.recordOf(
      "metadata",
      Schema.Field.of(DATABASE_TYPE, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(ACCESS_KEY_ID, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(SECRET_KEY_ID, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(REGION, Schema.of(Schema.Type.STRING)),
      Schema.Field.of(BUCKET_NAME, Schema.of(Schema.Type.STRING))
    );

    public S3Credentials(String accessKeyId, String secretKeyId, String region, String bucketName) {
      this.databaseType = S3FileMetadata.DATA_BASE_NAME;
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
      this.region = region;
      this.bucketName = bucketName;
    }
  }

  @Override
  public S3Credentials getCredentials() {
    return (S3Credentials) this.credentials;
  }

  @Override
  protected Schema getCredentialSchema() {
    return S3Credentials.CREDENTIAL_SCHEMA;
  }

  @Override
  protected void addCredentialsToBuilder(StructuredRecord.Builder builder) {
    builder
      .set(S3Credentials.DATABASE_TYPE, DATA_BASE_NAME)
      .set(S3Credentials.ACCESS_KEY_ID, getCredentials().accessKeyId)
      .set(S3Credentials.SECRET_KEY_ID, getCredentials().secretKeyId)
      .set(S3Credentials.REGION, getCredentials().region)
      .set(S3Credentials.BUCKET_NAME, getCredentials().bucketName);
  }

  @Override
  protected Credentials getCredentialsFromRecord(StructuredRecord record) {
    return new S3FileMetadata.S3Credentials((String) record.get(S3Credentials.ACCESS_KEY_ID),
                                            (String) record.get(S3Credentials.SECRET_KEY_ID),
                                            (String) record.get(S3Credentials.REGION),
                                            (String) record.get(S3Credentials.BUCKET_NAME));
  }
}
