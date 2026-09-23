package uk.gov.nationalarchives.ingestmapper

import cats.effect.IO
import cats.effect.IO.asyncForIO
import cats.effect.unsafe.implicits.global
import org.scalatest.Assertion
import org.scalatest.flatspec.AnyFlatSpec
import org.scalatest.matchers.should.Matchers.*
import sttp.capabilities.fs2.Fs2Streams
import sttp.client4.{Response, UriContext}
import sttp.client4.impl.cats.CatsMonadError
import sttp.client4.testing.StubBody.Exact
import sttp.client4.testing.{ResponseStub, WebSocketStreamBackendStub}
import sttp.model.StatusCode
import ujson.Obj
import uk.gov.nationalarchives.ingestmapper.DiscoveryService.{DiscoveryCollectionAsset, DiscoveryCollectionAssetResponse, DiscoveryScopeContent}

import java.net.URI
import java.util.UUID

class DiscoveryServiceTest extends AnyFlatSpec {

  val uuids: List[String] = List(
    "c7e6b27f-5778-4da8-9b83-1b64bbccbd03",
    "61ac0166-ccdf-48c4-800f-29e5fba2efda",
    "457cc27d-5b74-4e81-80e3-d808e0b3e425",
    "2b9e5c87-2342-4006-b1aa-5308a5ce2544",
    "a504d58d-2f7d-4f29-b5b8-173b558970db",
    "41c5604d-70b3-44d1-aa1f-d9ffe18b33cb"
  )

  lazy val s3Uri: URI = URI.create("s3://bucket/key")

  val uuidIterator: () => UUID = () => {
    val uuidsIterator: Iterator[String] = uuids.iterator
    UUID.fromString(uuidsIterator.next())
  }

  val baseUrl = "http://localhost"
  val assetMap: Map[String, Option[DiscoveryCollectionAsset]] = List("T", "T TEST").map { col =>
    col -> Option(DiscoveryCollectionAsset(col, DiscoveryScopeContent(Option(s"TestDescription $col 1          \nTestDescription $col 2")), Option(s"Test Title $col")))
  }.toMap
  val bodyMap: Map[String, Response[Exact]] = List("T", "T TEST").map { col =>
    val description = <scopecontent>
        <head>Head</head>
        <p><list>
          <item>TestDescription {col} &#49;</item>
          <item>TestDescription {col} &#50;</item></list>
        </p>
      </scopecontent>.toString().replaceAll("\n", "")

    val title = s"""<unittitle type=&#34Title\">Test \\Title $col</unittitle>""".stripMargin
    val assetResponse = DiscoveryCollectionAssetResponse(
      List(DiscoveryCollectionAsset(col, DiscoveryScopeContent(Option(description)), Option(title)))
    )
    col -> ResponseStub(Exact(Right(assetResponse)), StatusCode.Ok)
  }.toMap

  private def checkDynamoItem(item: Obj, collection: String, expectedId: String, parentPath: Option[String], citableRefFound: Boolean = true): Assertion = {
    val expectedTitle = if citableRefFound then s"Test Title $collection" else collection
    val expectedDescription = if citableRefFound then s"TestDescription $collection 1          \nTestDescription $collection 2" else ""

    item("id").str should equal(expectedId)
    item("name").str should equal(collection)
    item("batchId").str should equal("testBatch")
    item("type").str should equal("ArchiveFolder")
    !item.value.contains("fileSize") should be(true)
    item.value.get("parentPath").map(_.str) should equal(parentPath)
    if (collection != "Unknown") {
      item("title").str should equal(expectedTitle)
      item("id_Code").str should equal(collection)
      item("description").str should equal(expectedDescription)
    } else {
      item.value.contains("title") should equal(false)
      item.value.contains("id_Code") should equal(false)
      item.value.contains("description") should equal(false)
    }
  }

  "getAssetFromDiscoveryApi" should "return the correct values for series and department" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=TNA"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST?source=TNA"))
      .thenRespond(bodyMap("T TEST"))

    val departmentCollectionAsset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T")
      .unsafeRunSync()

    val seriesCollectionAsset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T TEST")
      .unsafeRunSync()

    def checkAsset(asset: DiscoveryCollectionAsset, suffix: String) = {
      asset.title.get should equal(s"Test Title $suffix")
      asset.scopeContent.description.get should equal(s"TestDescription $suffix 1          \nTestDescription $suffix 2")
      asset.citableReference should equal(suffix)
    }
    checkAsset(departmentCollectionAsset, "T")
    checkAsset(seriesCollectionAsset, "T TEST")
  }

  "getAssetFromDiscoveryApi" should "will not check the PA source if the TNA source returns assets" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=TNA"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=PA"))
      .thenRespondServerError()

    val asset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T")
      .unsafeRunSync()

    asset.citableReference should equal("T")
  }

  "getAssetFromDiscoveryApi" should "should check the PA source if the TNA source returns no assets" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=TNA"))
      .thenRespond(ResponseStub(Exact(Right(DiscoveryCollectionAssetResponse(Nil))), StatusCode.Ok))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=PA"))
      .thenRespond(bodyMap("T"))

    val asset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T")
      .unsafeRunSync()

    asset.citableReference should equal("T")
  }

  "getAssetFromDiscoveryApi" should "return the default asset if both PA and TNA sources return no assets" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=TNA"))
      .thenRespond(ResponseStub(Exact(Right(DiscoveryCollectionAssetResponse(Nil))), StatusCode.Ok))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=PA"))
      .thenRespond(ResponseStub(Exact(Right(DiscoveryCollectionAssetResponse(Nil))), StatusCode.Ok))

    val asset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T")
      .unsafeRunSync()

    asset.citableReference should equal("T")
    asset.scopeContent.description should equal(None)
    asset.title should equal(None)
  }

  "getAssetFromDiscoveryApi" should "should return the title and description unchanged if there is no xml in them" in {
    val response = DiscoveryCollectionAssetResponse(List(DiscoveryCollectionAsset("ref", DiscoveryScopeContent(Option("A description")), Option("A title"))))
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/ref?source=TNA"))
      .thenRespond(ResponseStub(Exact(Right(response)), StatusCode.Ok))

    val asset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("ref")
      .unsafeRunSync()

    asset.citableReference should equal("ref")
    asset.scopeContent.description.get should equal("A description")
    asset.title.get should equal("A title")
  }

  "getAssetFromDiscoveryApi" should "return an empty title and description if the discovery API returns an error" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError()).whenAnyRequest
      .thenRespondServerError()

    val discoveryAsset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("A")
      .unsafeRunSync()

    val seriesAsset = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("A TEST")
      .unsafeRunSync()

    discoveryAsset.title should equal(None)
    discoveryAsset.citableReference should equal("A")
    discoveryAsset.scopeContent.description should equal(None)

    seriesAsset.title should equal(None)
    seriesAsset.citableReference should equal("A TEST")
    seriesAsset.scopeContent.description should equal(None)
  }

  "getAssetFromDiscoveryApi" should "set the citable ref as the title and description as '', if the department call returns an empty asset" in {
    val emptyResponse: Response[Exact] = ResponseStub(Exact("""{"assets": []}"""), StatusCode.Ok)

    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=TNA"))
      .thenRespond(emptyResponse)
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST?source=TNA"))
      .thenRespond(bodyMap("T TEST"))

    val departmentItem = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T")
      .unsafeRunSync()

    val seriesItem = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T TEST")
      .unsafeRunSync()

    departmentItem.title should equal(None)
    departmentItem.scopeContent.description should equal(None)
    departmentItem.citableReference should equal("T")
    seriesItem.title.get should equal("Test Title T TEST")
    seriesItem.scopeContent.description.get should equal("TestDescription T TEST 1          \nTestDescription T TEST 2")
    seriesItem.citableReference should equal("T TEST")
  }

  "getAssetFromDiscoveryApi" should "set the citable ref as the title and description as '', if the series call returns an empty asset" in {
    val emptyResponse: Response[Exact] = ResponseStub(Exact("""{"assets": []}"""), StatusCode.Ok)

    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T?source=TNA"))
      .thenRespond(bodyMap("T"))
      .whenRequestMatches(_.uri.equals(uri"$baseUrl/API/records/v1/collection/T TEST?source=TNA"))
      .thenRespond(emptyResponse)

    val departmentItem = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T")
      .unsafeRunSync()

    val seriesItem = DiscoveryService(baseUrl, backend, uuidIterator)
      .getAssetFromDiscoveryApi("T TEST")
      .unsafeRunSync()

    departmentItem.title.get should equal("Test Title T")
    departmentItem.scopeContent.description.get should equal("TestDescription T 1          \nTestDescription T 2")
    departmentItem.citableReference should equal("T")
    seriesItem.title should equal(None)
    seriesItem.scopeContent.description should equal(None)
    seriesItem.citableReference should equal("T TEST")
  }

  "getDepartmentAndSeriesItems" should "return the correct values for series and department" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val discoveryService = DiscoveryService(baseUrl, backend, uuidIterator)

    val departmentItem = discoveryService.departmentItem("testBatch", assetMap("T"))
    val seriesItem = discoveryService.seriesItem("testBatch", departmentItem, assetMap("T TEST").get)

    checkDynamoItem(departmentItem, "T", uuids.head, None)
    checkDynamoItem(seriesItem, "T TEST", uuids.head, Option(uuids.head))
  }

  "getDepartmentAndSeriesItems" should "not add a title attribute if the title is missing" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
    val departmentCollectionAsset = assetMap("T").map(_.copy(title = None))
    val seriesCollectionAsset = assetMap("T TEST").map(_.copy(title = None))

    val discoveryService = DiscoveryService(baseUrl, backend, uuidIterator)
    val departmentItem = discoveryService.departmentItem("testBatch", departmentCollectionAsset)
    val seriesItem = discoveryService.seriesItem("testBatch", departmentItem, seriesCollectionAsset.get)

    departmentItem.value.contains("title") should equal(false)
    seriesItem.value.contains("title") should equal(false)
  }

  "getDepartmentAndSeriesItems" should "not add a description attribute if the description is missing" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())
    val departmentCollectionAsset = assetMap("T").map(departmentAsset => departmentAsset.copy(scopeContent = departmentAsset.scopeContent.copy(description = None)))
    val seriesCollectionAsset = assetMap("T TEST").map(seriesAsset => seriesAsset.copy(scopeContent = seriesAsset.scopeContent.copy(description = None)))

    val discoveryService = DiscoveryService(baseUrl, backend, uuidIterator)
    val departmentItem = discoveryService.departmentItem("testBatch", departmentCollectionAsset)
    val seriesItem = discoveryService.seriesItem("testBatch", departmentItem, seriesCollectionAsset.get)

    departmentItem.value.contains("description") should equal(false)
    seriesItem.value.contains("description") should equal(false)
  }

  "getDepartmentAndSeriesItems" should "return unknown for the department if the department is missing" in {
    val backend: WebSocketStreamBackendStub[IO, Fs2Streams[IO]] = WebSocketStreamBackendStub[IO, Fs2Streams[IO]](new CatsMonadError())

    val discoveryService = DiscoveryService(baseUrl, backend, uuidIterator)
    val departmentItem = discoveryService.departmentItem("testBatch", None)
    val seriesItem = discoveryService.seriesItem("testBatch", departmentItem, assetMap("T TEST").get)

    checkDynamoItem(departmentItem, "Unknown", uuids.head, None)
    checkDynamoItem(seriesItem, "T TEST", uuids.head, Option(uuids.head))
  }
}
