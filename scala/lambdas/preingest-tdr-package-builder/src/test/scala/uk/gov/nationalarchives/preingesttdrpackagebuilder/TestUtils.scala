package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.{IO, Ref}
import cats.syntax.all.*
import fs2.interop.reactivestreams.*
import io.circe.generic.semiauto.deriveEncoder
import io.circe.parser.decode
import io.circe.*
import org.reactivestreams.Publisher
import org.scanamo.DynamoFormat
import org.scanamo.request.RequestCondition
import reactor.core.publisher.Flux
import software.amazon.awssdk.core.async.SdkPublisher
import software.amazon.awssdk.services.dynamodb.model.BatchWriteItemResponse
import software.amazon.awssdk.services.s3.model.{DeleteObjectsResponse, HeadObjectResponse, ListObjectsV2Response, PutObjectResponse, PutObjectTaggingResponse, S3Object}
import software.amazon.awssdk.transfer.s3.model.{CompletedCopy, CompletedUpload}
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{IngestLockTableItem, checksumPrefix}
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.nio.ByteBuffer

object TestUtils:

  given Encoder[LockTableMessage] = deriveEncoder[LockTableMessage]

  given Encoder[PackageMetadata] = (m: PackageMetadata) => {
    val checksums = m.checksums.map { checksum =>
      (s"$checksumPrefix${checksum.algorithm}", Json.fromString(checksum.fingerprint))
    }
    val metadataObjectFields = List(
      ("Series", Json.fromString(m.series)).some,
      m.UUID.map(u => "UUID" -> Json.fromString(u.toString)),
      m.assetId.map(a => "AssetId" -> Json.fromString(a.toString)),
      ("fileId", Json.fromString(m.fileId.toString)).some,
      m.description.map(d => ("description", Json.fromString(d))),
      m.transferringBody.map(t => ("TransferringBody", Json.fromString(t))),
      m.transferInitiatedDatetime.map(dt => ("TransferInitiatedDatetime", Json.fromString(dt))),
      m.consignmentReference.map(c => ("ConsignmentReference", Json.fromString(c))),
      m.driBatchReference.map(d => ("driBatchReference", Json.fromString(d))),
      ("Filename", Json.fromString(m.filename)).some,
      ("FileReference", Json.fromString(m.fileReference)).some,
      ("ClientSideOriginalFilepath", Json.fromString(m.originalFilePath)).some,
      m.sortOrder.map(s => ("sortOrder", Json.fromInt(s))),
      m.digitalAssetSource.map(s => ("digitalAssetSource", Json.fromString(s))),
      m.formerRefDept.map(frd => ("formerRefDept", Json.fromString(frd))),
      m.formerRefTNA.map(frt => ("formerRefTNA", Json.fromString(frt))),
      m.IAID.map(iaid => ("IAID", Json.fromString(iaid))),
      m.citableRefPrefix.map(crp => ("CitableRefPrefix", Json.fromString(crp)))
    ).flatten ++ checksums
    Json.obj(metadataObjectFields*)
  }

  case class MockTdrFile(fileSize: Long)

  type S3Objects = String | List[MetadataObject] | MockTdrFile

  def mockDynamoClient(ref: Ref[IO, List[IngestLockTableItem]], failedQuery: Boolean = false): DADynamoDBClient[IO] = new DADynamoDBClient[IO]:
    override def deleteItems[T](tableName: String, primaryKeyAttributes: List[T])(using DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def writeItem(dynamoDbWriteRequest: DADynamoDBClient.DADynamoDbWriteItemRequest): IO[Int] = IO.pure(1)

    override def writeItems[T](tableName: String, items: List[T])(using format: DynamoFormat[T]): IO[List[BatchWriteItemResponse]] = IO.pure(Nil)

    override def queryItems[U](tableName: String, requestCondition: RequestCondition, potentialGsiName: Option[String])(using returnTypeFormat: DynamoFormat[U]): IO[List[U]] =
      if failedQuery then IO.raiseError(new Exception("Dynamo has returned an error"))
      else
        val groupId = for {
          dynamoValues <- Option(requestCondition.attributes.values)
          conditionValue <- dynamoValues.values.headOption
          value <- conditionValue.asString
        } yield value

        ref.get.map(_.filter(table => groupId.contains(table.groupId)).asInstanceOf[List[U]])

    override def getItems[T, K](primaryKeys: List[K], tableName: String)(using returnFormat: DynamoFormat[T], keyFormat: DynamoFormat[K]): IO[List[T]] = IO.pure(Nil)

    override def updateAttributeValues(dynamoDbRequest: DADynamoDBClient.DADynamoDbRequest): IO[Int] = IO.pure(1)

  def mockS3(ref: Ref[IO, Map[String, S3Objects]], downloadError: Boolean = false, uploadError: Boolean = false): DAS3Client[IO] = {
    new DAS3Client[IO]() {
      override def deleteObjects(bucket: String, keys: List[String]): IO[DeleteObjectsResponse] = IO.pure(DeleteObjectsResponse.builder.build)

      override def download(bucket: String, key: String): IO[Publisher[ByteBuffer]] =
        if downloadError then IO.raiseError(new Exception(s"Error downloading $key from S3 $bucket"))
        else
          for {
            fileMap <- ref.get
            metadata <- fileMap(key) match
              case metadata: String => IO.pure(metadata)
              case _                => IO.raiseError(new Exception("Expecting TDR Metadata Json"))
          } yield Flux.just(ByteBuffer.wrap(metadata.getBytes))

      override def headObject(bucket: String, key: String): IO[HeadObjectResponse] = ref.get.map { objectsMap =>
        val fileSize = objectsMap.get(key).collect({ case mockTdrFile: MockTdrFile => mockTdrFile }).get.fileSize
        HeadObjectResponse.builder.contentLength(fileSize).build
      }

      override def listCommonPrefixes(bucket: String, keysPrefixedWith: String): IO[SdkPublisher[String]] = IO.pure(SdkPublisher.fromIterable(java.util.List.of()))

      override def upload(bucket: String, key: String, publisher: Publisher[ByteBuffer]): IO[CompletedUpload] = {
        if uploadError then IO.raiseError(new Exception(s"Error uploading $key to $bucket"))
        else {
          for {
            jsonString <- publisher.toStreamBuffered[IO](1024).map(_.array().map(_.toChar).mkString).compile.string
            json <- IO.fromEither(decode[List[MetadataObject]](jsonString))
            _ <- ref.update(currentMap => currentMap + (key -> json))
          } yield CompletedUpload.builder.response(PutObjectResponse.builder.build).build
        }

      }

      override def copy(sourceBucket: String, sourceKey: String, destinationBucket: String, destinationKey: String): IO[CompletedCopy] = IO.pure(CompletedCopy.builder.build)

      override def listObjects(bucket: String, prefix: Option[String]): IO[ListObjectsV2Response] = ref.get.map { objects =>
        val s3Objects = objects.flatMap {
          case (key, file: MockTdrFile) => Option(S3Object.builder.key(key).size(file.fileSize).build)
          case _                        => None
        }.toList
        ListObjectsV2Response.builder.contents(s3Objects*).build
      }

      override def updateObjectTags(bucket: String, key: String, newTags: Map[String, String], potentialVersionId: Option[String]): IO[PutObjectTaggingResponse] = IO.stub
    }
  }
