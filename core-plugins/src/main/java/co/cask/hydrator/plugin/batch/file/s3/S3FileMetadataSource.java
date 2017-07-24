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

import co.cask.cdap.api.annotation.Description;
import co.cask.cdap.api.annotation.Macro;
import co.cask.cdap.api.annotation.Name;
import co.cask.cdap.api.annotation.Plugin;
import co.cask.cdap.api.data.batch.Input;
import co.cask.cdap.api.data.format.StructuredRecord;
import co.cask.cdap.api.data.schema.Schema;
import co.cask.cdap.api.dataset.lib.KeyValue;
import co.cask.cdap.etl.api.Emitter;
import co.cask.cdap.etl.api.batch.BatchRuntimeContext;
import co.cask.cdap.etl.api.batch.BatchSource;
import co.cask.cdap.etl.api.batch.BatchSourceContext;
import co.cask.hydrator.common.SourceInputFormatProvider;
import co.cask.hydrator.common.batch.JobUtils;
import co.cask.hydrator.plugin.batch.file.AbstractFileMetadata;
import co.cask.hydrator.plugin.batch.file.AbstractFileMetadataSource;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapreduce.Job;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import javax.annotation.Nullable;

/**
 * FileCopySource plugin that pulls filemetadata from S3 Filesystem.
 */
@Plugin(type = BatchSource.PLUGIN_TYPE)
@Name("S3FileMetadataSource")
@Description("Reads file metadata from S3 bucket.")
public class S3FileMetadataSource extends AbstractFileMetadataSource<S3FileMetadata> {
  private S3FileMetadataSourceConfig config;
  private static final Logger LOG = LoggerFactory.getLogger(S3FileMetadataSource.class);

  public S3FileMetadataSource(S3FileMetadataSourceConfig config) {
    super(config);
    this.config = config;
  }

  @Override
  public void prepareRun(BatchSourceContext context) throws Exception {
    Job job = JobUtils.createInstance();
    Configuration conf = job.getConfiguration();

    S3MetadataInputFormat.setSourcePaths(job, config.sourcePaths);
    S3MetadataInputFormat.setMaxSplitSize(job, config.maxSplitSize);
    S3MetadataInputFormat.setAccessKeyId(job, config.accessKeyId);
    S3MetadataInputFormat.setRecursiveCopy(job, config.recursiveCopy.toString());
    S3MetadataInputFormat.setSecretKeyId(job, config.secretKeyId);
    S3MetadataInputFormat.setRegion(job, config.region);
    S3MetadataInputFormat.setURI(job, "s3a://" + config.bucketName);
    S3MetadataInputFormat.setFsClass(job);
    S3MetadataInputFormat.setBucketName(job, config.bucketName);

    context.setInput(Input.of(config.referenceName, new SourceInputFormatProvider(S3MetadataInputFormat.class, conf)));
  }

  @Override
  public void initialize(BatchRuntimeContext context) throws Exception {
    super.initialize(context);
  }

  @Override
  public void transform(KeyValue<NullWritable, S3FileMetadata> input, Emitter<StructuredRecord> emitter) {
    emitter.emit(input.getValue().toRecord());
  }

  /**
   * Credentials required for connecting to S3Filesystem.
   */
  public class S3FileMetadataSourceConfig extends AbstractFileMetadataSourceConfig {

    // configurations for S3
    @Macro
    @Description("Your AWS Access Key Id")
    public String accessKeyId;

    @Macro
    @Description("Your AWS Secret Key Id")
    public String secretKeyId;

    @Macro
    @Nullable
    @Description("The AWS Region to operate in")
    public String region;

    @Macro
    @Description("The AWS Bucket Name to work with")
    public String bucketName;

    public S3FileMetadataSourceConfig(String name, String sourcePaths, int maxSplitSize,
                                  String accessKeyId, String secretKeyId, String region, String bucketName) {
      super(name, sourcePaths, maxSplitSize);
      this.accessKeyId = accessKeyId;
      this.secretKeyId = secretKeyId;
      this.region = region;
      this.bucketName = bucketName;
    }
  }
}
