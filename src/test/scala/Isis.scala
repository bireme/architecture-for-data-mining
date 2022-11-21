import isis.IsisDB
import java.io.FileNotFoundException
import org.mongodb.scala.bson._


class Isis extends munit.FunSuite {
  test("test isisdb tostring") {
    val iso_path = "/path/test.iso"
    val database = IsisDB(iso_path)
    assertEquals(database.toString(), iso_path)
  }

  test("test isis utils mouting error") {
    val iso_path = "/tmp/doesntexist.iso"
    val database = IsisDB(iso_path)

    val filefound = intercept[RuntimeException]{
      database.mount_mst()
    }

    assert(
      clue(filefound).getMessage.contains(
        "An exception occurred while trying to run the MX util to create"
      )
    )
  }

  test("mount and parse ISIS database") {
    val database = IsisDB("./src/test/data/testdb.iso")
    database.mount_mst()
    val documents = database.parse_data()
    assertEquals(documents.length, 10)

    val doc = documents(0)

    // All fields present?
    assert(doc.contains("1"))
    assert(doc.contains("2"))
    assert(doc.contains("3"))
    assert(doc.contains("4"))
    assert(doc.contains("5"))
    assert(doc.contains("6"))
    assert(doc.contains("7"))
    assert(doc.contains("9"))
    assert(doc.contains("17"))
    assert(doc.contains("18"))
    assert(doc.contains("20"))
    assert(doc.contains("40"))
    assert(doc.contains("62"))
    assert(doc.contains("64"))
    assert(doc.contains("65"))
    assert(doc.contains("66"))
    assert(doc.contains("67"))
    assert(doc.contains("84"))
    assert(doc.contains("87"))
    assert(doc.contains("88"))
    assert(doc.contains("90"))
    assert(doc.contains("91"))
    assert(doc.contains("92"))
    assert(doc.contains("98"))

    // Field that shouldn't exist
    assert(doc.contains("10") == false)

    // Checking some fields values
    assertEquals(doc.get("1").get.asString().getValue(), "CL27.1")
    assertEquals(doc.get("18").get.asString().getValue(), "Vertebral subluxation in chiropractic practice^ien")
    assertEquals(doc.get("91").get.asString().getValue(), "20020307")
  }
}
