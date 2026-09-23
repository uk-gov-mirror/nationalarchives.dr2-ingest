package uk.gov.nationalarchives.preingesttdrpackagebuilder

import cats.effect.unsafe.implicits.global
import cats.effect.{IO, Ref}
import fs2.Collector.string
import fs2.hashing.HashAlgorithm
import io.circe.syntax.*
import org.scalacheck.Gen
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import org.scalatestplus.scalacheck.ScalaCheckDrivenPropertyChecks
import uk.gov.nationalarchives.dynamoformatters.DynamoFormatters.{Checksum, IngestLockTableItem}
import uk.gov.nationalarchives.preingesttdrpackagebuilder.Lambda.*
import uk.gov.nationalarchives.preingesttdrpackagebuilder.TestUtils.{*, given}
import uk.gov.nationalarchives.utils.ExternalUtils.*
import uk.gov.nationalarchives.utils.ExternalUtils.SourceSystem.{ADHOC, TDR}
import uk.gov.nationalarchives.utils.NaturalSorting.{natural, given}

import java.net.URI
import java.time.{Instant, LocalDateTime, OffsetDateTime, ZoneOffset}
import java.time.format.DateTimeFormatter
import java.util.UUID
import scala.annotation.tailrec

class LambdaTest extends AnyFlatSpec with ScalaCheckDrivenPropertyChecks:
  val config: Config = Config("", "", "cacheBucket", 1, TDR)
  private val dateTimeNow: Instant = Instant.now()
  case class FileName(prefix: String, suffix: String) {
    def fileString: String = s"$prefix.$suffix"
  }

  private def checksum(fingerprint: String) = List(Checksum("sha256", fingerprint))

  case class TestData(
      fileId: UUID,
      assetId: Option[UUID],
      series: String,
      body: Option[String],
      date: Option[String],
      tdrRef: Option[String],
      fileName: FileName,
      fileSize: Long,
      checksums: List[Checksum],
      fileRef: String,
      groupId: String,
      batchId: String,
      filePath: String,
      driBatchRef: Option[String],
      description: Option[String],
      sortOrder: Option[Int],
      digitalAssetSource: Option[String],
      formerRefDept: Option[String],
      formerRefTNA: Option[String],
      iaid: Option[String],
      upstreamSystem: SourceSystem,
      filesPrefix: Option[String],
      citableRefPrefix: Option[String]
  )
  val dateGen: Gen[String] = for {
    year <- Gen.posNum[Int]
    month <- Gen.choose(1, 12)
    day <- Gen.choose(1, 28)
    hour <- Gen.choose(0, 23)
    minute <- Gen.choose(0, 59)
    second <- Gen.choose(0, 59)
  } yield DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss").format(LocalDateTime.of(year, month, day, hour, minute, second))

  val numericFileNameGen: Gen[FileName] = for {
    prefix <- Gen.asciiStr
    numPart <- Gen.listOfN(12, Gen.numChar).map(_.mkString)
    suffix <- Gen.asciiStr
  } yield FileName(s"$prefix$numPart", suffix)

  val stringFileNameGen: Gen[FileName] = for {
    prefix <- Gen.asciiStr
    suffix <- Gen.asciiStr
  } yield FileName(prefix, suffix)

  val fileNameGen: Gen[FileName] = Gen.oneOf(numericFileNameGen, stringFileNameGen)

  val nonZeroDigit: Gen[Int] = Gen.choose(1, 9)

  val formerRefTNAGen: Gen[String] = for {
    series <- Gen.listOfN(2, Gen.alphaUpperChar).map(_.mkString)
    num1 <- nonZeroDigit
    num2 <- nonZeroDigit
    num3 <- nonZeroDigit
  } yield s"$series $num1/$num2/$num3"

  val formerRefDeptGen: Gen[String] = Gen.alphaNumStr

  val checksumGen: Gen[Checksum] = for {
    algorithm <- Gen.alphaStr
    fingerprint <- Gen.alphaStr
  } yield Checksum(algorithm, fingerprint)

  val listTestCount: Iterator[Int] = (1 to 100).toList.iterator
  val tdrOrDriBatchGen: Gen[(Option[String], Option[String])] =
    for
      tdr <- Gen.option(Gen.asciiStr)
      dri <- Gen.option(Gen.asciiStr)
    yield if tdr.isEmpty && dri.isEmpty then (Option("TDR-123"), None) else (tdr, dri)

  val citableRefGen: Gen[String] =
    for
      slashCount <- Gen.choose(1, 6)
      parts <- Gen.listOfN(slashCount + 1, Gen.nonEmptyStringOf(Gen.alphaChar))
    yield parts.mkString("/")

  val testDataGen: Gen[TestData] = for {
    series <- Gen.asciiStr
    assetId <- Gen.option(Gen.uuid)
    body <- Gen.option(Gen.asciiStr)
    fileId <- Gen.uuid
    date <- Gen.option(dateGen)
    fileName <- fileNameGen
    fileSize <- Gen.posNum[Long]
    checksum <- checksumGen
    fileRef <- Gen.asciiStr
    groupId <- Gen.asciiStr
    batchId <- Gen.alphaStr
    filePath <- Gen.nonEmptyListOf(Gen.nonEmptyStringOf(Gen.alphaChar)).suchThat(_.size >= 2).map(s => s"/${s.mkString("/")}")
    (potentialTdrRef, potentialDriBatchRef) <- tdrOrDriBatchGen
    description <- Gen.option(Gen.nonEmptyStringOf(Gen.asciiChar))
    potentialSortOrder <- Gen.option(Gen.choose(0, 10))
    digitalAssetSource <- Gen.option(Gen.oneOf("Born Digital", "Surrogate"))
    formerRefDept <- Gen.option(formerRefDeptGen)
    formerRefTNA <- Gen.option(formerRefTNAGen)
    iaid <- Gen.option(Gen.nonEmptyStringOf(Gen.asciiChar))
    upstreamSystem <- Gen.oneOf(SourceSystem.values.toList)
    filesPrefix <- Gen.option(Gen.nonEmptyStringOf(Gen.alphaChar))
    citableRefPrefix <- Gen.option(citableRefGen)
  } yield TestData(
    fileId,
    assetId,
    series,
    body,
    date,
    potentialTdrRef,
    fileName,
    fileSize,
    List(checksum),
    fileRef,
    groupId,
    batchId,
    filePath,
    potentialDriBatchRef,
    description,
    potentialSortOrder,
    digitalAssetSource,
    formerRefDept,
    formerRefTNA,
    iaid,
    upstreamSystem,
    filesPrefix,
    citableRefPrefix
  )

  val testListDataGen: Gen[List[TestData]] = Gen.nonEmptyListOf(testDataGen)

  given PropertyCheckConfiguration = PropertyCheckConfiguration(minSuccessful = 100)

  forAll(testListDataGen) { allTestData =>
    "lambda handler for multiple files" should s"write the correct metadata to s3 ${listTestCount.next()}" in {
      runPropertiesTest(allTestData)
    }
  }

  private def runPropertiesTest(allTestData: List[TestData]) = {
    val archiveFolderId = UUID.fromString("dd28dde8-9d94-4843-8f46-fd3da71afaae")
    val fileIds = allTestData.map(_ => UUID.fromString("4e03a500-4f29-47c5-9c08-26e12f631fd8"))
    val metadataFileId = UUID.fromString("427789a1-e7af-4172-9fa7-02da1d60125f")
    val tdrFileId = UUID.fromString("a2834c9d-46e8-42d9-a300-2a4ed31c1e1a")
    val uuidList = List.fill(100)(UUID.randomUUID)
    val uuids: Iterator[UUID] = Iterator(uuidList*)
    val uuidIterator: () => UUID = () => uuids.next()
    def createPackageMetadata(testData: TestData) = PackageMetadata(
      testData.series,
      if testData.assetId.isDefined then None else Option(tdrFileId),
      testData.assetId,
      testData.fileId,
      testData.description,
      testData.body,
      testData.date,
      testData.tdrRef,
      s"${testData.fileName.prefix}.${testData.fileName.suffix}",
      testData.checksums,
      testData.fileRef,
      testData.filePath,
      testData.driBatchRef,
      testData.sortOrder,
      testData.digitalAssetSource,
      testData.formerRefDept,
      testData.formerRefTNA,
      testData.iaid,
      testData.citableRefPrefix
    )

    val testData = allTestData.head
    val packageMetadata: String = allTestData.map(createPackageMetadata).asJson.noSpaces

    val metadataChecksum = fs2.Stream
      .emits(packageMetadata.getBytes)
      .through(fs2.hashing.Hashing[IO].hash(HashAlgorithm.SHA256))
      .flatMap(hash => fs2.Stream.emits(hash.bytes.toList))
      .through(fs2.text.hex.encode)
      .compile
      .to(string)
      .unsafeRunSync()

    val expectedId = if testData.assetId.isDefined then testData.assetId.get else tdrFileId
    val expectedPrefix = if testData.filesPrefix.isDefined then testData.filesPrefix.get else expectedId

    val potentialLockTableMessageId = Some("messageId")
    val initialS3Objects =
      allTestData.map(testData => s"$expectedPrefix/${testData.fileId}" -> MockTdrFile(testData.fileSize)).toMap ++ Map(s"$expectedId.metadata" -> packageMetadata)

    val initialDynamoObjects = allTestData.map { testData =>
      val lockTableMessageAsString =
        new LockTableMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$expectedId.metadata"), testData.filesPrefix, potentialLockTableMessageId).asJson.noSpaces
      IngestLockTableItem(UUID.randomUUID(), testData.groupId, lockTableMessageAsString, dateTimeNow.toString)
    }
    val input = Input(testData.groupId, testData.batchId, 1, 2)
    val (s3Contents, output) = runHandler(uuidIterator, initialS3Objects, initialDynamoObjects, input, config = config.copy(sourceSystem = testData.upstreamSystem))

    def stripFileExtension(title: String) = if title.contains(".") then title.substring(0, title.lastIndexOf('.')) else title

    val metadataObjects: List[MetadataObject] = s3Contents(s"${testData.batchId}/metadata.json").asInstanceOf[List[MetadataObject]]
    val contentFolderMetadataObject = metadataObjects.collect { case contentFolderMetadataObject: ContentFolderMetadataObject => contentFolderMetadataObject }.head
    val assetMetadataObject = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }.head
    val fileMetadataObjects = metadataObjects.collect { case fileMetadataObject: FileMetadataObject => fileMetadataObject }
    val (metadataFileMetadataObject, fileObjects) = fileMetadataObjects.partition(_.name.endsWith("-metadata.json")) match {
      case (a, b) => (a.head, b)
    }
    val expectedContentFolderName = testData.upstreamSystem match {
      case SourceSystem.ADHOC => "Records"
      case _                  => testData.tdrRef.getOrElse(testData.driBatchRef.get)
    }
    val expectedTitle =
      if allTestData.length > 1 then
        allTestData.head.description match
          case Some(description) => truncate(description)
          case None              => "Untitled"
      else testData.fileName.fileString

    val expectedSeries = testData.citableRefPrefix match {
      case Some(crp) => crp.split("/").slice(0, 2).mkString("/")
      case None      => testData.series
    }

    contentFolderMetadataObject.name should equal(expectedContentFolderName)
    contentFolderMetadataObject.title should equal(None)
    contentFolderMetadataObject.parentId should equal(None)
    contentFolderMetadataObject.series should equal(Option(expectedSeries))

    assetMetadataObject.id should equal(expectedId)
    assetMetadataObject.parentId should equal(Option(uuidList(1)))
    assetMetadataObject.title should equal(expectedTitle)
    assetMetadataObject.name should equal(expectedId.toString)
    assetMetadataObject.originalMetadataFiles should equal(List(uuidList.head))
    assetMetadataObject.description should equal(testData.description)
    assetMetadataObject.transferringBody should equal(testData.body)
    checkDateTimeOptionFieldEquals(testData.date, assetMetadataObject.transferCompleteDatetime)
    assetMetadataObject.upstreamSystem.display should equal(testData.upstreamSystem.display)
    assetMetadataObject.digitalAssetSource should equal(testData.digitalAssetSource.getOrElse("Born Digital"))
    assetMetadataObject.digitalAssetSubtype should equal(None)
    assetMetadataObject.correlationId should equal(potentialLockTableMessageId)

    def checkDateTimeOptionFieldEquals(expected: Option[String], actual: Option[OffsetDateTime]) = {
      expected match {
        case Some(value) =>
          val expectedDate = LocalDateTime.parse(expected.get.replace(" ", "T")).atOffset(ZoneOffset.UTC)
          actual.get should equal(expectedDate)
        case None =>
          actual should be(None)
      }
    }

    def checkIdField(name: String, value: String): Unit =
      assetMetadataObject.idFields.find(_.name == name).map(_.value).get should equal(value)

    checkIdField("Code", s"${testData.citableRefPrefix.getOrElse(testData.series)}/${testData.fileRef}")
    if List(SourceSystem.DRI, SourceSystem.ADHOC).contains(testData.upstreamSystem)
    then checkIdField("UpstreamSystemReference", s"$expectedSeries/${testData.fileRef}")
    else if testData.upstreamSystem == SourceSystem.TDR then checkIdField("UpstreamSystemReference", testData.fileRef)

    if testData.upstreamSystem == SourceSystem.TDR then checkIdField("BornDigitalRef", testData.fileRef)
    testData.tdrRef.foreach(tdrRef => checkIdField("ConsignmentReference", tdrRef))
    checkIdField("RecordID", expectedId.toString)

    if SourceSystem.ADHOC == testData.upstreamSystem && testData.iaid.isDefined then checkIdField("DiscoveryIAID", testData.iaid.get)

    if testData.upstreamSystem == SourceSystem.DRI && testData.driBatchRef.isDefined && testData.tdrRef.isEmpty then checkIdField("DRIBatchReference", testData.driBatchRef.get)

    allTestData.sortBy(p => natural(p.fileName.fileString)).zipWithIndex.foreach { (testData, idx) =>
      val potentialFileObject = fileObjects.find(_.id == testData.fileId)
      potentialFileObject.isDefined should equal(true)
      val fileObject = potentialFileObject.get
      fileObject.parentId should equal(Option(expectedId))
      fileObject.title should equal(testData.fileName.fileString)
      fileObject.sortOrder should equal(testData.sortOrder.getOrElse(idx + 1))
      fileObject.name should equal(testData.fileName.fileString)
      fileObject.fileSize should equal(testData.fileSize)
      fileObject.representationType should equal(RepresentationType.Preservation)
      fileObject.representationSuffix should equal(1)
      fileObject.location should equal(URI.create(s"s3://bucket/$expectedPrefix/${testData.fileId}"))
      fileObject.checksums should equal(testData.checksums)
    }

    val metadataSortOrder = allTestData.flatMap(_.sortOrder).maxOption.getOrElse(fileObjects.size + 1)
    metadataFileMetadataObject.parentId should equal(Option(expectedId))
    metadataFileMetadataObject.title should equal(s"$expectedId-metadata")
    metadataFileMetadataObject.sortOrder should equal(metadataSortOrder)
    metadataFileMetadataObject.name should equal(s"$expectedId-metadata.json")
    metadataFileMetadataObject.fileSize should equal(packageMetadata.getBytes.length)
    metadataFileMetadataObject.representationType should equal(RepresentationType.Preservation)
    metadataFileMetadataObject.representationSuffix should equal(1)
    metadataFileMetadataObject.location should equal(URI.create(s"s3://bucket/$expectedId.metadata"))
    metadataFileMetadataObject.checksums.head should equal(Checksum("sha256", metadataChecksum))

    output.groupId should equal(testData.groupId)
    output.batchId should equal(testData.batchId)
    output.retryCount should equal(2)
    output.metadataPackage should equal(URI.create(s"s3://cacheBucket/${testData.batchId}/metadata.json"))
    output.retrySfnArn should equal("")
  }

  "lambda handler" should "create two content folders for two consignments with 10 files each" in {
    val archiveFolderId = UUID.fromString("dd28dde8-9d94-4843-8f46-fd3da71afaae")
    val fileId = UUID.fromString("4e03a500-4f29-47c5-9c08-26e12f631fd8")
    val metadataFileId = UUID.fromString("427789a1-e7af-4172-9fa7-02da1d60125f")
    val tdrAssetId = UUID.fromString("a2834c9d-46e8-42d9-a300-2a4ed31c1e1a")
    val uuids = Iterator(metadataFileId, fileId, archiveFolderId)
    val uuidIterator: () => UUID = () => UUID.randomUUID
    def packageMetadata(consignmentReference: String, fileId: UUID) = PackageMetadata(
      "TST 123",
      None,
      Option(tdrAssetId),
      fileId,
      None,
      Option("body"),
      Option("2024-10-18 00:00:01"),
      Option(consignmentReference),
      s"file.txt",
      checksum("checksum"),
      "reference",
      "/path/to/file.txt",
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ) :: Nil

    val groupId = UUID.randomUUID.toString
    val batchId = s"${groupId}_0"
    def ids = (1 to 10).toList.map { idx =>
      UUID.randomUUID -> UUID.randomUUID
    }
    val idsOne = ids
    val idsTwo = ids
    def createInitialS3Objects(idList: List[(UUID, UUID)], consignmentReference: String): Map[String, S3Objects] = Map.from(idList.flatMap { case (assetId, fileId) =>
      List(
        s"$assetId.metadata" -> packageMetadata(consignmentReference, fileId).asJson.noSpaces,
        s"$assetId/$fileId" -> MockTdrFile(1)
      )
    })
    val initialS3Objects = createInitialS3Objects(idsOne, "TDR-ABCD") ++ createInitialS3Objects(idsTwo, "TDR-EFGH")

    val initialDynamoObjects = (idsOne ++ idsTwo).map { case (assetId, _) =>
      val lockTableMessage = NotificationMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$assetId.metadata")).asJson.noSpaces
      IngestLockTableItem(UUID.randomUUID(), groupId, lockTableMessage, dateTimeNow.toString)
    }

    val input = Input(groupId, batchId, 1, 2)
    val (s3Contents, output) = runHandler(uuidIterator, initialS3Objects, initialDynamoObjects, input)

    val metadataObjects: List[MetadataObject] = s3Contents(s"$batchId/metadata.json").asInstanceOf[List[MetadataObject]]
    val assetMetadataObjects = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }
    val contentFolderMetadataObjects = metadataObjects.collect { case contentFolderMetadataObject: ContentFolderMetadataObject => contentFolderMetadataObject }

    contentFolderMetadataObjects.size should equal(2)

    assetMetadataObjects.count(_.parentId == Option(contentFolderMetadataObjects.head.id)) should equal(10)
    assetMetadataObjects.count(_.parentId == Option(contentFolderMetadataObjects.last.id)) should equal(10)

  }

  "lambda handler" should "create two content folders for two consignments two different series but the same consignment reference" in {
    val uuidIterator: () => UUID = () => UUID.randomUUID

    def packageMetadata(series: String, fileId: UUID) = PackageMetadata(
      series,
      None,
      Option(UUID.randomUUID),
      fileId,
      None,
      Option("body"),
      Option("2024-10-18 00:00:01"),
      Option("consignmentReference"),
      s"file.txt",
      checksum("checksum"),
      "reference",
      "/path/to/file.txt",
      None,
      None,
      None,
      None,
      None,
      None,
      None
    ) :: Nil

    val groupId = UUID.randomUUID.toString
    val batchId = s"${groupId}_0"

    def createInitialS3Objects(assetId: UUID, fileId: UUID, series: String): Map[String, S3Objects] =
      Map(
        s"$assetId.metadata" -> packageMetadata(series, fileId).asJson.noSpaces,
        s"$assetId/$fileId" -> MockTdrFile(1)
      )

    val (assetIdOne, assetIdTwo, fileIdOne, fileIdTwo) = (UUID.randomUUID, UUID.randomUUID, UUID.randomUUID, UUID.randomUUID)

    val initialS3Objects = createInitialS3Objects(assetIdOne, fileIdOne, "TST 123") ++ createInitialS3Objects(assetIdTwo, fileIdTwo, "TST 234")

    val initialDynamoObjects = List(assetIdOne, assetIdTwo).map { assetId =>
      val lockTableMessage = NotificationMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$assetId.metadata")).asJson.noSpaces
      IngestLockTableItem(UUID.randomUUID(), groupId, lockTableMessage, dateTimeNow.toString)
    }

    val input = Input(groupId, batchId, 1, 2)
    val (s3Contents, output) = runHandler(uuidIterator, initialS3Objects, initialDynamoObjects, input)

    val metadataObjects: List[MetadataObject] = s3Contents(s"$batchId/metadata.json").asInstanceOf[List[MetadataObject]]
    val assetMetadataObjects = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }
    val contentFolderMetadataObjects = metadataObjects.collect { case contentFolderMetadataObject: ContentFolderMetadataObject => contentFolderMetadataObject }

    contentFolderMetadataObjects.size should equal(2)

    contentFolderMetadataObjects.count(_.series.contains("TST 123")) should equal(1)
    contentFolderMetadataObjects.count(_.series.contains("TST 234")) should equal(1)

    assetMetadataObjects.count(_.parentId == Option(contentFolderMetadataObjects.head.id)) should equal(1)
    assetMetadataObjects.count(_.parentId == Option(contentFolderMetadataObjects.last.id)) should equal(1)
  }

  "lambda handler" should "return an error if the metadata list is empty" in {
    val ex = intercept[Exception] {
      runHandler()
    }
    ex.getMessage should equal("Metadata list for TST-123 is empty")
  }

  "lambda handler" should "return an error if the s3 download fails" in {
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key.metadata")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val ex = intercept[Throwable] {
      runHandler(initialDynamoObjects = initialDynamoObjects, downloadError = true)
    }
    ex.getMessage should equal("Error downloading key.metadata from S3 bucket")
  }

  "lambda handler" should "return an error if the s3 upload fails" in {
    val assetId = UUID.randomUUID
    val fileId = UUID.randomUUID
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$assetId.metadata")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val packageMetadata =
      List(
        PackageMetadata(
          "",
          Option(assetId),
          None,
          fileId,
          None,
          Option(""),
          Option("2024-10-04 10:00:00"),
          Option("ABC-123"),
          "test.txt",
          checksum(""),
          "",
          "",
          None,
          None,
          None,
          None,
          None,
          None,
          None
        )
      )
    val initialS3Objects = Map(s"$assetId.metadata" -> packageMetadata.asJson.noSpaces, s"$assetId/$fileId" -> MockTdrFile(1))
    val ex = intercept[Throwable] {
      runHandler(initialS3Objects = initialS3Objects, initialDynamoObjects = initialDynamoObjects, uploadError = true)
    }
    ex.getMessage should equal("Error uploading /metadata.json to cacheBucket")
  }

  "lambda handler" should "return a hardcoded value of 'Records' if neither the consignment reference nor DRI reference are set" in {
    val assetId = UUID.randomUUID
    val fileId = UUID.randomUUID
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$assetId.metadata")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val packageMetadata =
      List(
        PackageMetadata(
          "",
          Option(assetId),
          None,
          fileId,
          None,
          Option(""),
          Option("2024-10-04 10:00:00"),
          None,
          "test.txt",
          checksum(""),
          "",
          "",
          None,
          None,
          None,
          None,
          None,
          None,
          None
        )
      )
    val initialS3Objects = Map(s"$assetId.metadata" -> packageMetadata.asJson.noSpaces, s"$assetId/$fileId" -> MockTdrFile(1))
    val adhocConfig: Config = Config("", "", "cacheBucket", 1, ADHOC)
    val (s3Contents, output) = runHandler(initialS3Objects = initialS3Objects, initialDynamoObjects = initialDynamoObjects, config = adhocConfig)
    val metadataObjects: List[MetadataObject] = s3Contents(s"/metadata.json").asInstanceOf[List[MetadataObject]]
    val contentFolderMetadataObjects = metadataObjects.collect { case contentFolderMetadataObject: ContentFolderMetadataObject => contentFolderMetadataObject }
    contentFolderMetadataObjects.size should be(1)
    contentFolderMetadataObjects.head.name should be("Records")
  }

  "lambda handler" should "return upstream system of 'ADHOC' and correct former ref when the source system is ADHOC " in {
    val assetId = UUID.randomUUID
    val fileId = UUID.randomUUID
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$assetId.metadata")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val packageMetadata = List(
      PackageMetadata(
        "",
        Option(assetId),
        None,
        fileId,
        None,
        Option(""),
        Option("2024-10-04 10:00:00"),
        None,
        "test.txt",
        checksum(""),
        "",
        "",
        None,
        None,
        None,
        Some("TS245.ABCD.25-1"),
        Some("AB 8/4/6"),
        None,
        None
      )
    )
    val initialS3Objects = Map(s"$assetId.metadata" -> packageMetadata.asJson.noSpaces, s"$assetId/$fileId" -> MockTdrFile(1))
    val adhocConfig: Config = Config("", "", "cacheBucket", 1, ADHOC)
    val (s3Contents, output) = runHandler(initialS3Objects = initialS3Objects, initialDynamoObjects = initialDynamoObjects, config = adhocConfig)
    val metadataObjects: List[MetadataObject] = s3Contents(s"/metadata.json").asInstanceOf[List[MetadataObject]]
    val assetMetadataObjects = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }
    assetMetadataObjects.size should be(1)
    val assetMetadataObject = assetMetadataObjects.head
    assetMetadataObject.upstreamSystem should be(ADHOC)
    assetMetadataObject.idFields.find(_.name == "FormerRefDept").map(_.value).get should equal("TS245.ABCD.25-1")
    assetMetadataObject.idFields.find(_.name == "FormerRefTNA").map(_.value).get should equal("AB 8/4/6")
  }

  "lambda handler" should "not include former reference in the asset metadata when it is missing from package " in {
    val assetId = UUID.randomUUID
    val fileId = UUID.randomUUID
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create(s"s3://bucket/$assetId.metadata")).asJson.noSpaces
    val initialDynamoObjects = List(IngestLockTableItem(UUID.randomUUID(), "TST-123", lockTableMessageAsString, dateTimeNow.toString))

    val packageMetadata =
      List(
        PackageMetadata(
          "",
          None,
          Option(assetId),
          fileId,
          None,
          Option(""),
          Option("2024-10-04 10:00:00"),
          None,
          "test.txt",
          checksum(""),
          "",
          "",
          None,
          None,
          None,
          None,
          Some("AB 8/4/6"),
          None,
          None
        )
      )
    val initialS3Objects = Map(s"$assetId.metadata" -> packageMetadata.asJson.noSpaces, s"$assetId/$fileId" -> MockTdrFile(1))
    val adhocConfig: Config = Config("", "", "cacheBucket", 1, ADHOC)
    val (s3Contents, output) = runHandler(initialS3Objects = initialS3Objects, initialDynamoObjects = initialDynamoObjects, config = adhocConfig)
    val metadataObjects: List[MetadataObject] = s3Contents(s"/metadata.json").asInstanceOf[List[MetadataObject]]
    val assetMetadataObjects = metadataObjects.collect { case assetMetadataObject: AssetMetadataObject => assetMetadataObject }
    assetMetadataObjects.size should be(1)
    val assetMetadataObject = assetMetadataObjects.head
    assetMetadataObject.upstreamSystem should be(ADHOC)
    assetMetadataObject.idFields should not contain "FormerRefDept"
    assetMetadataObject.idFields.find(_.name == "FormerRefTNA").map(_.value).get should equal("AB 8/4/6")
  }

  "lambda handler" should "return an error if the dynamo query fails" in {
    val lockTableMessageAsString = new LockTableMessage(UUID.randomUUID(), URI.create("s3://bucket/key")).asJson.noSpaces

    val ex = intercept[Throwable] {
      runHandler(queryError = true)
    }
    ex.getMessage should equal("Dynamo has returned an error")
  }

  private def runHandler(
      uuidIterator: () => UUID = () => UUID.randomUUID,
      initialS3Objects: Map[String, S3Objects] = Map.empty,
      initialDynamoObjects: List[IngestLockTableItem] = Nil,
      input: Input = Input("TST-123", "", 1, 1),
      downloadError: Boolean = false,
      uploadError: Boolean = false,
      queryError: Boolean = false,
      config: Config = config
  ): (Map[String, S3Objects], Output) = {
    (for {
      initialS3Objects <- Ref.of[IO, Map[String, S3Objects]](initialS3Objects)
      initialDynamoObjects <- Ref.of[IO, List[IngestLockTableItem]](initialDynamoObjects)
      dependencies = Dependencies(mockDynamoClient(initialDynamoObjects, queryError), mockS3(initialS3Objects, downloadError, uploadError), uuidIterator)
      output <- new Lambda().handler(input, config, dependencies)
      s3Objects <- initialS3Objects.get
    } yield (s3Objects, output)).unsafeRunSync()
  }

  private def truncate(s: String) = {
    @tailrec
    def truncateArray(arr: Array[String]): Array[String] =
      if arr.isEmpty || arr.dropRight(1).mkString(" ").length < 100 then arr
      else truncateArray(arr.dropRight(1))

    if s.length < 100 then s else s"${truncateArray(s.split(" ")).mkString(" ")}..."
  }
