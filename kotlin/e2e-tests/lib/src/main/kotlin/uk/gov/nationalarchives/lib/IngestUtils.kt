package uk.gov.nationalarchives.lib

import aws.sdk.kotlin.services.cloudwatchlogs.CloudWatchLogsClient
import aws.sdk.kotlin.services.cloudwatchlogs.model.LiveTailSessionLogEvent
import aws.sdk.kotlin.services.cloudwatchlogs.model.StartLiveTailRequest
import aws.sdk.kotlin.services.cloudwatchlogs.model.StartLiveTailResponseStream
import aws.sdk.kotlin.services.dynamodb.DynamoDbClient
import aws.sdk.kotlin.services.dynamodb.batchGetItem
import aws.sdk.kotlin.services.dynamodb.model.AttributeValue
import aws.sdk.kotlin.services.dynamodb.model.KeysAndAttributes
import aws.sdk.kotlin.services.dynamodb.model.PutItemRequest
import aws.sdk.kotlin.services.s3.S3Client
import aws.sdk.kotlin.services.s3.getObjectTagging
import aws.sdk.kotlin.services.s3.listObjectVersions
import aws.sdk.kotlin.services.s3.model.GetObjectRequest
import aws.sdk.kotlin.services.s3.model.PutObjectRequest
import aws.sdk.kotlin.services.sfn.SfnClient
import aws.sdk.kotlin.services.sfn.model.DescribeExecutionRequest
import aws.sdk.kotlin.services.sfn.model.DescribeExecutionResponse
import aws.sdk.kotlin.services.sfn.model.ExecutionStatus
import aws.sdk.kotlin.services.sfn.model.StartExecutionRequest
import aws.sdk.kotlin.services.sqs.SqsClient
import aws.sdk.kotlin.services.sqs.model.SendMessageRequest
import aws.smithy.kotlin.runtime.content.ByteStream
import aws.smithy.kotlin.runtime.content.decodeToString
import com.typesafe.config.Config
import kotlinx.coroutines.*
import kotlinx.coroutines.flow.takeWhile
import uk.gov.nationalarchives.lib.JsonUtils.ExternalNotificationMessage
import uk.gov.nationalarchives.lib.JsonUtils.Parser
import uk.gov.nationalarchives.lib.JsonUtils.Payload
import uk.gov.nationalarchives.lib.JsonUtils.TDRMetadata
import uk.gov.nationalarchives.lib.JsonUtils.TDRParams
import uk.gov.nationalarchives.lib.JsonUtils.TREMetadata
import uk.gov.nationalarchives.lib.JsonUtils.TREMetadataParameters
import uk.gov.nationalarchives.lib.JsonUtils.TREParams
import uk.gov.nationalarchives.lib.JsonUtils.ValidationErrorMessage
import uk.gov.nationalarchives.lib.JsonUtils.jsonCodec
import uk.gov.nationalarchives.lib.JsonUtils.AdhocMetadata
import uk.gov.nationalarchives.lib.JsonUtils.DRIMetadata
import java.net.URI
import java.security.MessageDigest
import java.time.LocalDate
import java.time.OffsetDateTime
import java.time.ZonedDateTime
import java.time.format.DateTimeFormatter
import java.util.*
import java.util.concurrent.TimeoutException
import kotlin.random.Random
import kotlin.time.Duration
import kotlin.time.Duration.Companion.milliseconds
import kotlin.time.Duration.Companion.minutes
import kotlin.time.Duration.Companion.seconds

class IngestUtils(
    private val sqsClient: SqsClient,
    private val s3Client: S3Client,
    private val cloudWatchLogsClient: CloudWatchLogsClient,
    private val dynamoDbClient: DynamoDbClient,
    private val sfnClient: SfnClient,
    private val config: Config,
    private val assetIds: MutableList<UUID>,
    private val groupIds: MutableSet<String>
) {
    private val completeStatus = "Asset has been written to tape"
    private val failedStatus = "There has been an error ingesting the asset."

    suspend fun waitForEntriesInLockTable(timeout: Duration = 15.minutes): MutableSet<String> {
        val pollInterval = 30 * 1000
        val pendingAssetIds = assetIds.map { it.toString() }.toMutableSet()
        val tableName = config.getString("lockTable")
        try {
            withTimeout(timeout) {
                while (pendingAssetIds.isNotEmpty()) {
                    val rows = pendingAssetIds
                        .chunked(100)
                        .flatMap { assetIdsChunk -> getBatchRows(tableName, assetIdsChunk) }
                    rows.forEach { row ->
                        row["assetId"]?.asS()?.let { pendingAssetIds.remove(it) }
                        row["groupId"]?.asS()?.takeIf { it.isNotBlank() }?.let { groupIds.add(it) }
                    }
                    if (pendingAssetIds.isNotEmpty()) {
                        delay(pollInterval.milliseconds)
                    }
                }
            }
        } catch (_: TimeoutCancellationException) {
            throw TimeoutException("Timed out waiting for lock table entries")
        }
        return groupIds
    }

    fun checkForValidationFailureMessages(sourceSystemName: String, timeout: Long) {
        val sourceSystem = SourceSystem.valueOf(sourceSystemName.uppercase())
        val logGroupArn = sourceSystem.getCopyFilesLogGroup(config)
        streamLogs(timeout, logGroupArn) { logEvents: List<LiveTailSessionLogEvent>? ->
            logEvents?.let { events ->
                val assetIdsFromMessage = events
                    .flatMap {
                        try {
                            listOf(jsonCodec.decodeFromString<ValidationErrorMessage>(it.message!!))
                        } catch (_: Exception) {
                            emptyList()
                        }
                    }
                    .map {
                        UUID.fromString(it.assetId ?:it.s3FolderName)
                    }
                assetIds.removeAll(assetIdsFromMessage)
                assetIds.isEmpty()
            } ?: false
        }
    }


    fun checkForIngestStatusMessages(logGroupArn: String, timeout: Long, messageType: String) {
        val status = if (messageType == "update") failedStatus else completeStatus
        streamLogs(timeout, logGroupArn) { logEvents: List<LiveTailSessionLogEvent>? ->
            logEvents?.let { events ->
                val assetIdsFromMessage = events
                    .map { jsonCodec.decodeFromString<ExternalNotificationMessage>(it.message!!) }
                    .filter { it.body.properties.messageType == "preserve.digital.asset.ingest.$messageType" && it.body.parameters.status == status }
                    .map { it.body.parameters.assetId }
                assetIds.removeAll(assetIdsFromMessage)
                assetIds.isEmpty()
            } ?: false
        }
    }

    private suspend fun describeStepFunction(groupId: String): DescribeExecutionResponse {
        val stepFunctionName = "${groupId}_0"
        val describeRequest = DescribeExecutionRequest {
            executionArn = "arn:aws:states:eu-west-2:${config.getString("accountNumber")}:execution:${config.getString("ingestSfnName")}:$stepFunctionName"
        }
        return sfnClient.describeExecution(describeRequest)
    }

    suspend fun checkStepFunctionCompletes(timeout: Duration = 15.minutes) {
        groupIds.forEach { groupId ->
            try {
                var status: ExecutionStatus? = null
                withTimeout(timeout) {
                    while (status != ExecutionStatus.Succeeded) {
                        val response = describeStepFunction(groupId)
                        status = response.status
                        if (status != ExecutionStatus.Succeeded) {
                            delay(30.seconds)
                        }
                    }
                    val response = describeStepFunction(groupId)
                    val sfnInput = response.input?.let { input -> jsonCodec.decodeFromString<JsonUtils.SfnInput>(input)}
                    val metadataBucket = sfnInput?.metadataPackage?.host
                    val metadataPrefix = sfnInput?.metadataPackage?.path?.drop(1)
                    val versionResponse = s3Client.listObjectVersions {
                        bucket = metadataBucket
                        prefix = metadataPrefix
                    }
                    if (versionResponse.deleteMarkers.isNullOrEmpty()) {
                        throw Exception("Metadata file has not been deleted for group id $groupId")
                    }
                    val metadataVersionId = versionResponse.versions?.first()?.versionId
                    val getObjectRequest = GetObjectRequest {
                        bucket = metadataBucket
                        key = metadataPrefix
                        versionId = metadataVersionId
                    }
                    s3Client.getObject(getObjectRequest) { resp ->
                        val ingestMetadata = resp.body?.let {body -> body.decodeToString().let { bodyString -> jsonCodec.decodeFromString<List<JsonUtils.IngestMetadata>>(bodyString) } }
                        do {
                            val notDeletedFiles = ingestMetadata
                                ?.filter { metadata -> metadata.type == "File" }
                                ?.mapNotNull { metadata -> metadata.location }
                                ?.filter { location ->
                                    val taggingResponse = s3Client.getObjectTagging {
                                        bucket = location.host
                                        key = location.path.drop(1)
                                    }
                                    !taggingResponse.tagSet.map { tag -> tag.key }.contains("TO_BE_DELETED")
                                }
                            if (!notDeletedFiles.isNullOrEmpty()) {
                                delay(30.seconds)
                            }
                        } while (!notDeletedFiles.isNullOrEmpty())
                    }
                }
            } catch (_: TimeoutCancellationException) {
                throw TimeoutException("Timed out waiting for step function completion for group id $groupId")
            }
        }
    }

    suspend fun createFiles(
        numberOfFiles: Int,
        sourceSystemName: String = "TDR",
        emptyChecksum: Boolean = false,
        invalidMetadata: Boolean = false,
        invalidChecksum: Boolean = false
    ) {
        val sourceSystem = SourceSystem.valueOf(sourceSystemName.uppercase())
        val invalidChecksumValue = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"
        assetIds.addAll(List<UUID>(numberOfFiles) { UUID.randomUUID() })
        coroutineScope {
            assetIds.mapIndexed { index, assetId ->
                launch {
                    val body = if (index == 0) assetId.toString().repeat(600000) // This makes one file which is large enough to test multipart copy
                    else assetId.toString()
                    val checksum = if (emptyChecksum) ""
                    else if (invalidChecksum) invalidChecksumValue
                    else hash(body)
                    val fileId = UUID.randomUUID()

                    uploadPackage(sourceSystem, assetId, fileId, body, checksum, invalidMetadata)
                }
            }
        }
    }

    private fun idToRef(id: UUID): String = id.toString().split("-").first()

    //This identifies as a Word doc in DROID.
    private val wordDocBytes = byteArrayOf(
        0x50, 0x4B, 0x03, 0x04,  0x14, 0x00, 0x00, 0x00, 0x00, 0x00, 0x37, 0x4F,  0x5A, 0x5A, 0xDC.toByte(), 0xA7.toByte(),
        0x7C, 0x06, 0x3A, 0x08,  0x00, 0x00, 0x3A, 0x08,0x00, 0x00, 0x11, 0x00,  0x00, 0x00, 0x77, 0x6F,
        0x72, 0x64, 0x2F, 0x64,  0x6F, 0x63, 0x75, 0x6D, 0x65, 0x6E, 0x74, 0x2E,  0x78, 0x6D, 0x6C, 0x78,
        0x6D, 0x6C, 0x6E, 0x73,  0x3A, 0x77, 0x3D, 0x22, 0x68, 0x74, 0x74, 0x70,  0x3A, 0x2F, 0x2F, 0x70,
        0x75, 0x72, 0x6C, 0x2E,  0x6F, 0x63, 0x6C, 0x63, 0x2E, 0x6F, 0x72, 0x67,  0x2F, 0x6F, 0x6F, 0x78,
        0x6D, 0x6C, 0x2F, 0x77,  0x6F, 0x72, 0x64, 0x70, 0x72, 0x6F, 0x63, 0x65,  0x73, 0x73, 0x69, 0x6E,
        0x67, 0x6D, 0x6C, 0x2F,  0x6D, 0x61, 0x69, 0x6E, 0x22
    )

    private fun createJudgmentMetadata(id: UUID, invalid: Boolean): ByteArray {
        val tdrUuid = if (invalid) null else id
        val parser = Parser("http://example.com/id/ijkl/2025/1/doc-type/3", "cite", "test", listOf(), listOf())
        val treParams = TREParams(idToRef(id), Payload("test.docx"))
        val checksum = "d315c315347b08cccbf38d48d54f24afa7f3d7c7740a86fdc85e2832f6367f95"
        val tdrParams = TDRParams(checksum, "TDR", "id", OffsetDateTime.now(), idToRef(id), tdrUuid)
        return jsonCodec.encodeToString(TREMetadata(TREMetadataParameters(parser, treParams, tdrParams))).toByteArray()
    }

    suspend fun createJudgment(id: UUID, invalid: Boolean) {
        val bucketName = SourceSystem.JUDGMENT.getBucket(config)
        val metadataBytes = createJudgmentMetadata(id, invalid)
        val batchRef = idToRef(id)
        val s3FolderName = id.toString()
        uploadFileToS3(bucketName, "$s3FolderName/data/test.docx", ByteStream.fromBytes(wordDocBytes))
        uploadFileToS3(bucketName, "$s3FolderName/TRE-$batchRef-metadata.json", ByteStream.fromBytes(metadataBytes))
    }

    suspend fun sendImportMessages(sourceSystemName: String) = coroutineScope {
        val sourceSystem = SourceSystem.valueOf(sourceSystemName.uppercase())
        val queue = sourceSystem.getImporterQueue(config)
        
        assetIds.map {
            async {
                val request = SendMessageRequest {
                    queueUrl = queue
                    messageBody = buildMessageBody(sourceSystem, it)
                }
                sqsClient.sendMessage(request)
            }
        }
    }
    
    fun buildMessageBody(sourceSystem: SourceSystem, id: UUID): String {
        val bucket = sourceSystem.getBucket(config)
        if (sourceSystem == SourceSystem.JUDGMENT) {
            val inputParameters = JsonUtils.TREInputParameters(idToRef(id), id.toString(), "TDR", bucket, "COURT_DOCUMENT_PARSE_NO_ERRORS", true)
            return jsonCodec.encodeToString(JsonUtils.TREInput(inputParameters))
        } else {
            val metadataLocation = URI.create("s3://$bucket/$id.metadata")
            return jsonCodec.encodeToString(JsonUtils.SqsInputMessage(id, bucket, metadataLocation))
        }
    }

    suspend fun createBatch() = coroutineScope {
        val groupId = "E2E_${UUID.randomUUID()}"
        val batchId = "${groupId}_0"
        assetIds.map {
            async {
                val input = jsonCodec.encodeToString(
                    JsonUtils.AggregatorInputMessage(it, URI.create("s3://${config.getString("s3Bucket")}/$it.metadata"))
                )
                val request = PutItemRequest {
                    conditionExpression = "attribute_not_exists(assetId)"
                    tableName = config.getString("lockTable")
                    item = mapOf(
                        "assetId" to AttributeValue.S(it.toString()),
                        "groupId" to AttributeValue.S(groupId),
                        "message" to AttributeValue.S(input),
                        "createdAt" to AttributeValue.S(DateTimeFormatter.ISO_DATE_TIME.format(ZonedDateTime.now())),
                    )
                }
                dynamoDbClient.putItem(request)
            }.join()
        }
        val sfnInput = jsonCodec.encodeToString(JsonUtils.SFNArguments(groupId, batchId, 0, 2))
        val startExecutionRequest = StartExecutionRequest {
            stateMachineArn = config.getString("preingestSfnArn")
            input = sfnInput
            name = batchId
        }
        sfnClient.startExecution(startExecutionRequest)

    }

    private fun hash(body: String): String {
        val bytes = body.toByteArray()
        val md = MessageDigest.getInstance("SHA-256")
        val digest = md.digest(bytes)
        return digest.fold("") { str, it -> str + "%02x".format(it) }
    }

    private fun makeReference(length: Int): String {
        val chars = ('A'..'Z') + ('0'..'9')
        return chars.shuffled(Random).take(length).joinToString("")
    }

    private suspend fun uploadPackage(sourceSystem: SourceSystem, assetId: UUID, fileId: UUID, body: String, checksum: String, invalidMetadata: Boolean) {
        val thisYear = LocalDate.now().year
        fun <T> generateValue(value: T): T? = if (invalidMetadata && Random.nextBoolean()) null else value

        fun generateSeries() = listOf(null, "TEST123", "").shuffled().first()
        val series = if (invalidMetadata) generateSeries() else "TEST 123"
        val citableRefPrefix = if (Math.random() > 0.5) null else series?.replace(" ", "/")
        val bucketName = sourceSystem.getBucket(config)

        suspend fun uploadNonJudgmentPackage(metadata: String) {
            uploadFileToS3(bucketName,"$assetId/${fileId}", ByteStream.fromString(body))
            uploadFileToS3(bucketName,"${assetId}.metadata", ByteStream.fromString(metadata))
        }

        when (sourceSystem) {
            SourceSystem.TDR -> uploadNonJudgmentPackage(jsonCodec.encodeToString(listOf(TDRMetadata(
                series,
                generateValue(assetId),
                null,
                "TestBody",
                generateValue("2024-10-07 09:54:48"),
                "E2E-${thisYear}-${makeReference(4)}",
                "${assetId}.txt",
                checksum,
                "Z${makeReference(5)}",
                "/",
                fileId,
                citableRefPrefix
            ))))
            SourceSystem.ADHOC -> uploadNonJudgmentPackage(jsonCodec.encodeToString(listOf(AdhocMetadata(
                series,
                generateValue(assetId),
                fileId,
                null,
                "${assetId}.txt",
                "Z${makeReference(5)}",
                "/",
                "C12345678",
                "some:file",
                "SS 1/2/34",
                checksum
            ))))
            SourceSystem.DRI -> uploadNonJudgmentPackage(jsonCodec.encodeToString(listOf(DRIMetadata(
                series,
                generateValue(assetId),
                fileId,
                null,
                generateValue("2026-05-05 09:54:48"),
                "${assetId}.txt",
                "Z${makeReference(5)}",
                "some_metadata",
                "/",
                "Unknown",
                1,
                "C12345678",
                "someref",
                checksum
            ))))
            SourceSystem.JUDGMENT -> createJudgment(assetId, invalidMetadata)
        }
    }

    private suspend fun uploadFileToS3(bucketName: String, objectKey: String, bodyStream: ByteStream) {
        val request = PutObjectRequest {
            bucket = bucketName
            key = objectKey
            body = bodyStream
        }
        s3Client.putObject(request)
    }

    private suspend fun getBatchRows(tableName: String, assetIds: List<String>): List<Map<String, AttributeValue>> {
        val resp = dynamoDbClient.batchGetItem {
            requestItems = mapOf(
                tableName to KeysAndAttributes {
                    keys = assetIds.map { assetId ->
                        mapOf("assetId" to AttributeValue.S(assetId))
                    }
                }
            )
        }
        return resp.responses?.get(tableName).orEmpty()
    }

    private fun streamLogs(timeout: Long, logGroup: String, isComplete: (List<LiveTailSessionLogEvent>?) -> Boolean) =
        runBlocking {
            val request = StartLiveTailRequest {
                logGroupIdentifiers = listOf(logGroup)
            }

            cloudWatchLogsClient.startLiveTail(request) { response ->
                response.responseStream?.let { stream ->
                    try {
                        withTimeout(timeout.milliseconds) {
                            stream.takeWhile { value ->
                                when (value) {
                                    is StartLiveTailResponseStream.SessionUpdate -> {
                                        !isComplete(value.asSessionUpdate().sessionResults!!)
                                    }
                                    else -> true
                                }
                            }.collect {
                                it.asSessionUpdateOrNull()?.sessionResults
                            }
                        }
                    } catch (_: TimeoutCancellationException) {
                        throw TimeoutException("Timed out")
                    }
                }
            }
        }

    enum class SourceSystem {
        TDR {
            override fun getBucket(config: Config): String {return config.getString("s3Bucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("sqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String { return config.getString("copyFilesLogGroup") }
        }, 
        JUDGMENT {
            override fun getBucket(config: Config): String {return config.getString("s3Bucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("judgmentSqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String { return config.getString("copyJudgmentFilesLogGroup")}
        }, 
        ADHOC {
            override fun getBucket(config: Config): String {return config.getString( "adhocBucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("adhocSqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String { return config.getString("copyAdhocFilesLogGroup") }
        },
        DRI {
            override fun getBucket(config: Config): String {return config.getString( "driBucket")}
            override fun getImporterQueue(config: Config): String { return config.getString("driSqsQueue") }
            override fun getCopyFilesLogGroup(config: Config): String { return config.getString("copyDRIFilesLogGroup") }
        };
    
        abstract fun getBucket(config: Config): String
        abstract fun getImporterQueue(config: Config): String
        abstract fun getCopyFilesLogGroup(config: Config): String
    }
}
