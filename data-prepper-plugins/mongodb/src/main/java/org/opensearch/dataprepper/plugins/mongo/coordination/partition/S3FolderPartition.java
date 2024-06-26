/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.dataprepper.plugins.mongo.coordination.partition;

import org.opensearch.dataprepper.model.source.coordinator.SourcePartitionStoreItem;
import org.opensearch.dataprepper.model.source.coordinator.enhanced.EnhancedSourcePartition;

import java.util.Optional;

/**
 * A S3 partition represents an S3 partition job to create S3 path prefix/sub folder that will
 * be used to group records based on record key.
 */
public class S3FolderPartition extends EnhancedSourcePartition<String> {

    public static final String PARTITION_TYPE = "S3_FOLDER";
    private final String bucketName;
    private final String subFolder;
    private final String region;
    private final String collection;

    public S3FolderPartition(final SourcePartitionStoreItem sourcePartitionStoreItem) {
        setSourcePartitionStoreItem(sourcePartitionStoreItem);
        String[] keySplits = sourcePartitionStoreItem.getSourcePartitionKey().split("\\|");
        collection = keySplits[0];
        bucketName = keySplits[1];
        subFolder = keySplits[2];
        region = keySplits[3];
    }

    public S3FolderPartition(final String bucketName, final String subFolder, final String region, final String collection) {
        this.bucketName = bucketName;
        this.subFolder = subFolder;
        this.region = region;
        this.collection = collection;
    }
    
    @Override
    public String getPartitionType() {
        return PARTITION_TYPE;
    }

    @Override
    public String getPartitionKey() {
        return collection + "|" + bucketName + "|" + subFolder + "|" + region;
    }

    @Override
    public Optional<String> getProgressState() {
        return Optional.empty();
    }


    public String getBucketName() {
        return bucketName;
    }

    public String getSubFolder() {
        return subFolder;
    }

    public String getRegion() {
        return region;
    }

    public String getCollection() {
        return collection;
    }
}
