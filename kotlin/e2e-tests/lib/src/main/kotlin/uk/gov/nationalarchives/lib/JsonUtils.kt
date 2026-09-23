package uk.gov.nationalarchives.lib

import kotlinx.serialization.Contextual
import kotlinx.serialization.KSerializer
import kotlinx.serialization.Serializable
import kotlinx.serialization.descriptors.PrimitiveKind
import kotlinx.serialization.descriptors.PrimitiveSerialDescriptor
import kotlinx.serialization.descriptors.SerialDescriptor
import kotlinx.serialization.encoding.Decoder
import kotlinx.serialization.encoding.Encoder
import kotlinx.serialization.json.Json
import kotlinx.serialization.modules.SerializersModule
import java.net.URI
import java.time.OffsetDateTime
import java.util.*

object JsonUtils {

    object UUIDSerializer : KSerializer<UUID> {
        override val descriptor = PrimitiveSerialDescriptor("UUID", PrimitiveKind.STRING)

        override fun deserialize(decoder: Decoder): UUID = UUID.fromString(decoder.decodeString())

        override fun serialize(encoder: Encoder, value: UUID) = encoder.encodeString(value.toString())
    }

    object URISerializer : KSerializer<URI> {
        override val descriptor = PrimitiveSerialDescriptor("URI", PrimitiveKind.STRING)

        override fun deserialize(decoder: Decoder): URI = URI.create(decoder.decodeString())

        override fun serialize(encoder: Encoder, value: URI) = encoder.encodeString(value.toString())
    }
    
    object OffsetDateTimeSerializer : KSerializer<OffsetDateTime> {
        override val descriptor: SerialDescriptor = PrimitiveSerialDescriptor("OffsetDateTime", PrimitiveKind.STRING)

        override fun deserialize(decoder: Decoder): OffsetDateTime {
            return decoder.decodeString().let { dateTime -> OffsetDateTime.parse(dateTime) }
        }

        override fun serialize(encoder: Encoder, value: OffsetDateTime) = encoder.encodeString(value.toString())
    }

    @Serializable
    data class TDRMetadata(
        val Series: String?,
        @Contextual val UUID: UUID?,
        val description: String?,
        val TransferringBody: String,
        val TransferInitiatedDatetime: String?,
        val ConsignmentReference: String,
        val Filename: String,
        val SHA256ServerSideChecksum: String,
        val FileReference: String,
        val ClientSideOriginalFilepath: String,
        @Contextual val fileId: UUID,
        val CitableRefPrefix: String?
    )

    val jsonCodec = Json {
        serializersModule = SerializersModule {
            contextual(UUID::class, UUIDSerializer)
            contextual(URI::class, URISerializer)
            contextual(OffsetDateTime::class, OffsetDateTimeSerializer)
        }
        ignoreUnknownKeys = true
    }

    @Serializable
    data class SFNArguments(val groupId: String, val batchId: String, val waitFor: Int, val retryCount: Int)

    @Serializable
    data class AggregatorInputMessage(@Contextual val id: UUID, @Contextual val location: URI)

    @Serializable
    data class ValidationErrorMessage(val error: String, val assetId: String? = null, val s3FolderName: String? = null)

    @Serializable
    data class SqsInputMessage(@Contextual val fileId: UUID, val bucket: String, @Contextual val metadataLocation: URI)

    @Serializable
    data class ExternalNotificationMessage(val body: ExternalNotificationBody)

    @Serializable
    data class ExternalNotificationBody(
        val properties: ExternalNotificationProperties,
        val parameters: ExternalNotificationParameters
    )

    @Serializable
    data class ExternalNotificationParameters(
        @Serializable(with = UUIDSerializer::class) val assetId: UUID,
        val status: String
    )

    @Serializable
    data class ExternalNotificationProperties(val executionId: String, val messageType: String)

    @Serializable
    data class Parser(
        val uri: String?,
        val cite: String?,
        val name: String?,
        val attachments: List<String>,
        val `error-messages`: List<String>
    )

    @Serializable
    data class Payload(val filename: String)

    @Serializable
    data class TREParams(val reference: String, val payload: Payload)

    @Serializable
    data class TDRParams(
        val `Document-Checksum-sha256`: String,
        val `Source-Organization`: String,
        val `Internal-Sender-Identifier`: String,
        @Contextual val `Consignment-Export-Datetime`: OffsetDateTime,
        val `File-Reference`: String?,
        @Contextual val `UUID`: UUID?
    )

    @Serializable
    data class TREMetadataParameters(val PARSER: Parser, val TRE: TREParams, val TDR: TDRParams)

    @Serializable
    data class TREMetadata(val parameters: TREMetadataParameters)

    @Serializable
    data class TREInputParameters(
        val reference: String,
        val s3FolderName: String,
        val originator: String,
        val s3Bucket: String,
        val status: String,
        val skipSeriesLookup: Boolean
    )

    @Serializable
    data class TREInput(val parameters: TREInputParameters)

    @Serializable
    data class AdhocMetadata(
        val Series: String?,
        @Contextual val UUID: UUID?,
        @Contextual val fileId: UUID,
        val description: String?,
        val Filename: String,
        val FileReference: String,
        val ClientSideOriginalFilepath: String,
        val IAID: String,
        val formerRefDept: String?,
        val formerRefTNA: String?,
        val checksum_sha256: String
    )
    
    @Serializable
    data class DRIMetadata(
        val Series : String?,
        @Contextual val UUID: UUID?,
        @Contextual val fileId: UUID,
        val description: String?,
        val TransferInitiatedDatetime: String?,
        val Filename: String,
        val FileReference: String,
        val metadata: String,
        val ClientSideOriginalFilepath: String,
        val digitalAssetSource: String,
        val sortOrder: Int,
        val IAID: String,
        val driBatchReference: String,
        val checksum_sha256: String
    )

    @Serializable
    data class IngestMetadata(
        val type: String,
        @Contextual val location: URI? = null
    )

    @Serializable
    data class SfnInput(@Contextual val metadataPackage: URI)
}