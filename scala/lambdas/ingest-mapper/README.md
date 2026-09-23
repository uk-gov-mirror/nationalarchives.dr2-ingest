# DR2 Ingest - Mapper

This Lambda reads a JSON file passed in as the input to the Lambda, parses file contents and writes this to a DynamoDB
table.

The lambda:

* Reads the input from the step function step with this format:

```json
{
  "batchId": "batch",
  "metadataPackage": "s3://metadata-bucket/metadata.json",
  "executionName": "executionName"
}
```

* Downloads the metadata json file from the [metadataPackage](/docs/metadataPackage.md) location and parses it.
* Gets the parent objects from this file (the objects with an empty `parentId`)
* Gets a list of series names from these parent objects. Extracts the department reference from the series reference by
  splitting the series by spaces and taking only the first part.
* For each unique series and department pair, gets the title and description from Discovery. 
  We first try to call Discovery with `source=TNA` to check for TNA entries. If there are no assets returned, we try `source=PA` to check for Parliament entries.  
  This is run through the
  XSLT in `src/main/resources/transform.xsl` to replace the Encoded Archival Description (EAD) tags with newlines.
  The title and description may not be in EAD format. If this is the case, the unmodified title and description are added to the table.
  If Discovery is unavailable, or the
  series doesn't yet exist, the title and description are not added to the table.
* If the series is `Unknown`, it will create a hierarchy of `Unknown/`.
* Creates a [ujson](https://www.lihaoyi.com/post/uJsonfastflexibleandintuitiveJSONforScala.html) object, `Obj`. We use a generic `Obj` because we
  will eventually have to handle fields we don't know about in advance. This json `Obj` contains
  * batchId (from the input)
  * the `parentPath` of each entity, replacing the `parentId`.
  * the SHA256 checksum from the metadata json.
  * file extension.
  * the `childCount` of each entity.
  * the metadata json fields (except for the `parentId`).
  * the department and series output.
  * a ttl which is set to `${TTL_DAYS}` days in the future.
* This ujson `Obj` is written to DynamoDB.
* Write the UUIDs for the folders (Content and Archive) and Assets, to separate json files, in an S3 bucket with the
  paths `<executionName>/folders.json`, `<executionName>/assets.json`, respectively.
* Writes the state data (including information on the json files with the ids) for the next step function step with this
  format:

```json
{
  "groupId": "TDR-2023-ABC",
  "batchId": "TDR-2023-ABC_0",
  "metadataPackage": "s3://metadata-bucket/metadata.json",
  "assets": {
    "bucket": "stateBucketName",
    "key": "<executionName>/assets.json"
  },
  "folders": {
    "bucket": "stateBucketName",
    "key": "<executionName>/folders.json"
  },
  "archiveHierarchyFolders": [
    "f0d3d09a-5e3e-42d0-8c0d-3b2202f0e176",
    "e88e433a-1f3e-48c5-b15f-234c0e663c27",
    "93f5a200-9ee7-423d-827c-aad823182ad2"
  ],
  "totalAssetCount":  5,
  "totalFileBytes": 1000
}
```
Note: `totalFileBytes` is the sum of the bytes of all files (including the metadata.json)

## Environment Variables

| Name               | Description                                                  |
|--------------------|--------------------------------------------------------------|
| FILES_DDB_TABLE    | The table to write the values to                             |
| OUTPUT_BUCKET_NAME | The bucket to write the JSON files (containing the UUIDs) to |
| TTL_DAYS           | The number of days in the future to set the TTL on the rows  |
