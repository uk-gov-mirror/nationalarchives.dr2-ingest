package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.IO
import cats.effect.std.AtomicCell
import fs2.Collector.string
import fs2.Stream
import fs2.hashing.{HashAlgorithm, Hashing}
import fs2.interop.reactivestreams.*
import io.circe
import io.circe.fs2.{decoder as fs2Decoder, *}
import io.circe.generic.auto.*
import io.circe.parser.decode
import io.circe.syntax.*
import io.circe.{Decoder, HCursor}
import org.reactivestreams.FlowAdapters
import org.scanamo.syntax.*
import pureconfig.ConfigReader
import software.amazon.awssdk.services.s3.model.S3Object
import uk.gov.nationalarchives.DADynamoDBClient.given
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.given
import uk.gov.nationalarchives.utils.ExternalUtils.RepresentationType.Preservation
import uk.gov.nationalarchives.utils.{ExternalUtils, LambdaRunner}
import uk.gov.nationalarchives.utils.NaturalSorting.{natural, given}
import uk.gov.nationalarchives.{DADynamoDBClient, DAS3Client}

import java.net.URI
import java.nio.ByteBuffer
import java.nio.file.Path
import java.time.{LocalDateTime, ZoneOffset}
import java.util.UUID
import scala.annotation.tailrec
import scala.jdk.CollectionConverters.*

class Lambda extends LambdaRunner[Input, Output, Config, Dependencies]:
  lazy private val bufferSize = 1024 * 5
  private val defaultFolderName = "Records"
  private val formerRefTnaIdKey = "FormerRefTNA"
  private val formerRefDeptIdKey = "FormerRefDept"
  private val upstreamSystemRefIdKey = "UpstreamSystemReference"
  private val discoveryIaidKey = "DiscoveryIAID"

  override def handler: (Input, Config, Dependencies) => IO[Output] = (input, config, dependencies) => {

    def processNonMetadataObjects(
        metadataArr: Array[Byte],
        fileLocation: URI,
        metadataId: UUID,
        potentialMessageId: Option[String],
        contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]],
        potentialFilesPrefix: Option[String]
    ): IO[List[MetadataObject]] = {
      val jsonString = new String(metadataArr, "utf-8")
      decodePackageMetadata(jsonString)
        .flatMap { packageMetadataList =>
          def createMetadataObjects(firstPackageMetadata: PackageMetadata, fileName: String, originalFilePath: String) = {
            val series = getSeries(firstPackageMetadata)
            for {
              assetMetadata <- createAsset(firstPackageMetadata, fileName, originalFilePath, metadataId, potentialMessageId)
              s3FilesMap <- listS3Objects(fileLocation.getHost, potentialFilesPrefix.getOrElse(assetMetadata.id.toString))
              contentFolderKey <- config.sourceSystem match {
                case SourceSystem.ADHOC => IO.pure(s"$series$defaultFolderName")
                case _                  =>
                  IO.fromOption[String](firstPackageMetadata.consignmentReference.orElse(firstPackageMetadata.driBatchReference))(
                    new Exception(s"We need either a consignment reference or DRI batch reference for ${assetMetadata.id}")
                  ).map(reference => s"$series$reference")
              }
              metadataObjects <- contentFolderCell.modify[List[MetadataObject]] { contentFolderMap =>
                val fileMetadataObjs: List[FileMetadataObject] = packageMetadataList.sortBy(p => natural(p.filename)).zipWithIndex.map { (packageMetadata, idx) =>
                  val s3File = s3FilesMap(packageMetadata.fileId)
                  FileMetadataObject(
                    packageMetadata.fileId,
                    Option(assetMetadata.id),
                    packageMetadata.filename,
                    packageMetadata.sortOrder.getOrElse(idx + 1),
                    packageMetadata.filename,
                    s3File.size(),
                    Preservation,
                    1,
                    URI.create(s"s3://${fileLocation.getHost}/${s3File.key()}"),
                    packageMetadata.checksums
                  )
                }

                val contentFolderName = contentFolderKey.substring(series.length)
                val potentialContentFolder = contentFolderMap.get(contentFolderKey)
                if potentialContentFolder.isDefined then (contentFolderMap, assetMetadata.copy(parentId = potentialContentFolder.map(_.id)) :: fileMetadataObjs)
                else
                  val contentFolderId = dependencies.uuidGenerator()
                  val contentFolderMetadata = ContentFolderMetadataObject(contentFolderId, None, None, contentFolderName, Option(series), Nil)
                  val updatedMap = contentFolderMap + (contentFolderKey -> contentFolderMetadata)
                  val allMetadata = List(contentFolderMetadata, assetMetadata.copy(parentId = Option(contentFolderMetadata.id))) ++ fileMetadataObjs
                  (updatedMap, allMetadata)
              }
            } yield metadataObjects
          }

          packageMetadataList match {
            case head :: Nil  => createMetadataObjects(head, head.filename, head.originalFilePath)
            case head :: rest => createMetadataObjects(head, descriptionToFileName(head.description), getParentPath(head.originalFilePath))
            case Nil          => IO.raiseError(new Exception("The metadata list is empty"))
          }
        }
    }

    def metadataSha256Fingerprint(metadataFileBytes: Array[Byte]) = Stream
      .emits(metadataFileBytes)
      .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
      .flatMap(hash => Stream.emits(hash.bytes.toList))
      .through(fs2.text.hex.encode)
      .compile
      .to(string)

    def processMetadataFiles(metadataArr: Array[Byte], fileLocation: URI, metadataId: UUID): IO[MetadataObject] = {
      val metadataJsonString = new String(metadataArr, "utf-8")
      for {
        packageMetadataList <- decodePackageMetadata(metadataJsonString)
        sha256Fingerprint <- metadataSha256Fingerprint(metadataArr)
        firstPackageMetadata <- IO.fromOption(packageMetadataList.headOption)(new Exception("The metadata list is empty"))
      } yield {
        val metadataFileSize = metadataArr.length
        val potentialAssetId = firstPackageMetadata.assetId.orElse(firstPackageMetadata.UUID)
        FileMetadataObject(
          metadataId,
          potentialAssetId,
          s"${potentialAssetId.get}-metadata",
          packageMetadataList.flatMap(_.sortOrder).maxOption.getOrElse(packageMetadataList.length + 1),
          s"${potentialAssetId.get}-metadata.json",
          metadataFileSize,
          Preservation,
          1,
          fileLocation,
          List(Checksum("sha256", sha256Fingerprint))
        )
      }
    }

    def processPackageMetadata(
        metadataByteArr: Array[Byte],
        fileLocation: URI,
        potentialMessageId: Option[String],
        contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]],
        potentialFilesPrefix: Option[String]
    ): IO[List[MetadataObject]] = {
      val metadataId = dependencies.uuidGenerator()
      for {
        nonMetadataObjects <- processNonMetadataObjects(metadataByteArr, fileLocation, metadataId, potentialMessageId, contentFolderCell, potentialFilesPrefix)
        metadataFiles <- processMetadataFiles(metadataByteArr, fileLocation, metadataId)
      } yield metadataFiles :: nonMetadataObjects
    }

    def downloadMetadataFile(lockTableMessage: LockTableMessage, contentFolderCell: AtomicCell[IO, Map[String, ContentFolderMetadataObject]]): IO[List[MetadataObject]] = {
      val fileLocation = lockTableMessage.location
      val potentialMessageId = lockTableMessage.messageId
      dependencies.s3Client
        .download(fileLocation.getHost, fileLocation.getPath.drop(1))
        .map(_.toStreamBuffered[IO](bufferSize))
        .flatMap(_.compile.toList)
        .map(_.toArray.flatMap(_.array()))
        .flatMap { metadataByteArray =>
          processPackageMetadata(metadataByteArray, fileLocation, potentialMessageId, contentFolderCell, lockTableMessage.filesPrefix)
        }
    }

    def processLockTableItems(lockTableItems: List[IngestLockTableItem]): IO[Unit] = {
      AtomicCell[IO].of[Map[String, ContentFolderMetadataObject]](Map()).flatMap { contentFolderCell =>
        Stream
          .emits(lockTableItems)
          .map(_.message)
          .through(stringStreamParser[IO])
          .through(fs2Decoder[IO, LockTableMessage])
          .parEvalMap[IO, List[MetadataObject]](config.concurrency)(lockTableMessage => downloadMetadataFile(lockTableMessage, contentFolderCell))
          .compile
          .toList
          .map(_.flatten)
          .flatMap { metadata =>
            IO.raiseWhen(metadata.isEmpty)(new Exception(s"Metadata list for ${input.groupId} is empty")) >> {
              val metadataBytes = metadata.asJson.noSpaces.getBytes
              Stream.emits(metadataBytes).chunks.map(_.toByteBuffer).toPublisherResource[IO, ByteBuffer].use { publisher =>
                dependencies.s3Client.upload(config.rawCacheBucket, s"${input.batchId}/metadata.json", FlowAdapters.toPublisher(publisher)) >> IO.unit
              }
            }
          }
      }
    }

    def listS3Objects(bucket: String, prefix: String): IO[Map[UUID, S3Object]] = {
      dependencies.s3Client.listObjects(bucket, Option(prefix)).map { res =>
        res
          .contents()
          .asScala
          .toList
          .filterNot(_.key().endsWith(".metadata"))
          .map { s3Object =>
            UUID.fromString(s3Object.key.split("/").last) -> s3Object
          }
          .toMap
      }
    }

    def createAsset(
        packageMetadata: PackageMetadata,
        fileName: String,
        originalFilePath: String,
        metadataId: UUID,
        potentialMessageId: Option[String]
    ): IO[AssetMetadataObject] = IO.pure {
      val assetId = packageMetadata.assetId.getOrElse(packageMetadata.UUID.get)
      val series = getSeries(packageMetadata)
      val sourceSpecificIdentifiers = config.sourceSystem match {
        case SourceSystem.TDR => List(IdField("BornDigitalRef", packageMetadata.fileReference), IdField(upstreamSystemRefIdKey, packageMetadata.fileReference))
        case SourceSystem.DRI =>
          List(IdField(upstreamSystemRefIdKey, s"$series/${packageMetadata.fileReference}")) ++
            packageMetadata.driBatchReference.map(driBatchRef => IdField("DRIBatchReference", driBatchRef)).toList ++
            packageMetadata.IAID.map(iaid => IdField(discoveryIaidKey, iaid)).toList
        case SourceSystem.ADHOC =>
          List(IdField(upstreamSystemRefIdKey, s"$series/${packageMetadata.fileReference}")) ++
            packageMetadata.formerRefDept.map(frd => List(IdField(formerRefDeptIdKey, frd))).getOrElse(Nil) ++
            packageMetadata.formerRefTNA.map(frt => List(IdField(formerRefTnaIdKey, frt))).getOrElse(Nil) ++
            packageMetadata.IAID.map(iaid => IdField(discoveryIaidKey, iaid)).toList
        case _ => Nil
      }
      val digitalAssetSource = packageMetadata.digitalAssetSource.getOrElse("Born Digital")
      AssetMetadataObject(
        assetId,
        None,
        fileName,
        assetId.toString,
        List(metadataId),
        packageMetadata.description,
        packageMetadata.transferringBody,
        packageMetadata.transferInitiatedDatetime.map { dt => LocalDateTime.parse(dt.replace(" ", "T")).atOffset(ZoneOffset.UTC) },
        config.sourceSystem,
        digitalAssetSource,
        None,
        originalFilePath,
        potentialMessageId,
        List(
          IdField(
            "Code",
            s"${packageMetadata.citableRefPrefix.getOrElse(packageMetadata.series)}/${packageMetadata.fileReference}"
          ),
          IdField("RecordID", assetId.toString)
        ) ++ sourceSpecificIdentifiers ++ packageMetadata.consignmentReference.map(consignmentRef => List(IdField("ConsignmentReference", consignmentRef))).getOrElse(Nil)
      )
    }

    dependencies.dynamoDbClient
      .queryItems(config.lockTableName, "groupId" === input.groupId, Option(config.lockTableGsiName))
      .flatMap(processLockTableItems)
      .map(_ => new Output(input.batchId, input.groupId, URI.create(s"s3://${config.rawCacheBucket}/${input.batchId}/metadata.json"), input.retryCount, ""))
  }

  private def truncate(s: String) = {
    @tailrec
    def truncateArray(arr: Array[String]): Array[String] =
      if arr.isEmpty || arr.dropRight(1).mkString(" ").length < 100 then arr
      else truncateArray(arr.dropRight(1))
    if s.length < 100 then s else s"${truncateArray(s.split(" ")).mkString(" ")}..."
  }

  private def getSeries(packageMetadata: PackageMetadata) =
    packageMetadata.citableRefPrefix match {
      case Some(citableRefPrefix) => citableRefPrefix.split("/").slice(0, 2).mkString("/")
      case None                   => packageMetadata.series
    }

  private def descriptionToFileName(description: Option[String]) =
    description match
      case Some(value) => truncate(value)
      case None        => "Untitled"

  private def getParentPath(path: String) =
    Option(Path.of(path).getParent).map(_.toString).getOrElse(path)

  private def decodePackageMetadata(json: String): IO[List[PackageMetadata]] =
    IO.fromEither(decode[List[PackageMetadata]](json))

  override def dependencies(config: Config): IO[Dependencies] =
    IO(Dependencies(DADynamoDBClient[IO](), DAS3Client[IO](), () => UUID.randomUUID()))
end Lambda

object Lambda:

  given Decoder[PackageMetadata] = (c: HCursor) =>
    for
      series <- c.downField("Series").as[String]
      uuid <- c.downField("UUID").as[Option[UUID]]
      assetId <- c.downField("AssetId").as[Option[UUID]]
      fileId <- c.downField("fileId").as[UUID]
      description <- c.downField("description").as[Option[String]]
      transferringBody <- c.downField("TransferringBody").as[Option[String]]
      transferInitiatedDatetime <- c.downField("TransferInitiatedDatetime").as[Option[String]]
      consignmentReference <- c.downField("ConsignmentReference").as[Option[String]]
      fileName <- c.downField("Filename").as[String]
      checksums <- getChecksums(c)
      fileReference <- c.downField("FileReference").as[String]
      filePath <- c.downField("ClientSideOriginalFilepath").as[String]
      driBatchReference <- c.downField("driBatchReference").as[Option[String]]
      sortOrder <- c.downField("sortOrder").as[Option[Int]]
      digitalAssetSource <- c.downField("digitalAssetSource").as[Option[String]]
      formerRefDept <- c.downField("formerRefDept").as[Option[String]]
      formerRefTNA <- c.downField("formerRefTNA").as[Option[String]]
      iaid <- c.downField("IAID").as[Option[String]]
      citableRefPrefix <- c.downField("CitableRefPrefix").as[Option[String]]
    yield PackageMetadata(
      series,
      uuid,
      assetId,
      fileId,
      description,
      transferringBody,
      transferInitiatedDatetime,
      consignmentReference,
      fileName,
      checksums,
      fileReference,
      filePath,
      driBatchReference,
      sortOrder,
      digitalAssetSource,
      formerRefDept,
      formerRefTNA,
      iaid,
      citableRefPrefix
    )

  case class PackageMetadata(
      series: String,
      UUID: Option[UUID],
      assetId: Option[UUID],
      fileId: UUID,
      description: Option[String],
      transferringBody: Option[String],
      transferInitiatedDatetime: Option[String],
      consignmentReference: Option[String],
      filename: String,
      checksums: List[Checksum],
      fileReference: String,
      originalFilePath: String,
      driBatchReference: Option[String],
      sortOrder: Option[Int],
      digitalAssetSource: Option[String],
      formerRefDept: Option[String],
      formerRefTNA: Option[String],
      IAID: Option[String],
      citableRefPrefix: Option[String]
  )

  type LockTableMessage = NotificationMessage

  case class Config(lockTableName: String, lockTableGsiName: String, rawCacheBucket: String, concurrency: Int, sourceSystem: SourceSystem) derives ConfigReader

  case class Dependencies(dynamoDbClient: DADynamoDBClient[IO], s3Client: DAS3Client[IO], uuidGenerator: () => UUID)

  case class Input(groupId: String, batchId: String, waitFor: Int, retryCount: Int = 0)

  type Output = StepFunctionInput
