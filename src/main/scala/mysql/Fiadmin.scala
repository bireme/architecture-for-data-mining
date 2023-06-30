package mysql
import config.Settings
import java.sql.{Connection,DriverManager,PreparedStatement}


/**
  * An abstraction to handle Fi-Admin data in MySQL
  */
object Fiadmin:
  val driver = "com.mysql.cj.jdbc.Driver"

  val url = Settings.getConf("FIADMIN_MYSQL_URL")
  val username = Settings.getConf("FIADMIN_MYSQL_USER")
  val password = Settings.getConf("FIADMIN_MYSQL_PASSWORD")
  var connection : Connection = _

  val url_source = Settings.getConf("FIADMIN_MYSQL_SOURCE_URL")
  val username_source = Settings.getConf("FIADMIN_MYSQL_SOURCE_USER")
  val password_source = Settings.getConf("FIADMIN_MYSQL_SOURCE_PASSWORD")
  var connection_source : Connection = _

  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
    connection_source = DriverManager.getConnection(url_source, username_source, password_source)
  } catch {
    case e: Exception => e.printStackTrace
  }

  /**
    * Queries the Fi-Admin MySQL database for the
    * database (usually v4 in ISIS) ID.
    * Each request is cached for optimization.
    *
    * @param database
    * @return the database ID in Fi-Admin
    */
  var database_id_cache: Map[String, String] = Map()
  def get_database_id(database: String): String =
    var id: String = null

    if (database_id_cache.contains(database)) {
      id = database_id_cache(database)
    } else {
      val sql = "select id from database_database where acronym = ?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, database)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          id = rs.getString("id")
        }
        database_id_cache += database -> id
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return id

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * Auxiliary Code is valid.
    * Each request is cached for optimization.
    *
    * @param code 
    * @return if the Auxiliary Code is valid
    */
  var is_code_valid_cache: Map[String, Boolean] = Map()
  def is_code_valid(code: String, field: String): Boolean =
    var is_code_valid: Boolean = false

    val cache_key = s"$field$code"
    if (is_code_valid_cache.contains(cache_key)) {
      is_code_valid = is_code_valid_cache(cache_key)
    } else {
      val sql = "SELECT id FROM utils_auxcode WHERE field=? and code=?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, field)
      statement.setString(2, code)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          is_code_valid = true
        }
        is_code_valid_cache += cache_key -> is_code_valid
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return is_code_valid

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * Country Code is valid.
    * Each request is cached for optimization.
    *
    * @param country_code
    * @return if the Country Code is valid
    */
  var get_country_code_cache: Map[String, String] = Map()
  def get_country_code(country: String): String =
    var id: String = null

    if (get_country_code_cache.contains(country)) {
      id = get_country_code_cache(country)
    } else {
      val sql = "select code from utils_country left join utils_countrylocal on utils_country.id=country_id where utils_country.name=? OR utils_countrylocal.name=?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, country)
      statement.setString(2, country)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          id = rs.getString("code")
        }
        get_country_code_cache += country -> id
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return id

  /**
    * Queries the Fi-Admin MySQL database for the Source ID 
    * for a SAS document.
    * Status priority = 1, 0, -3, -1
    * Each request is cached for optimization.
    */
  var get_source_sas_id_cache: Map[String, String] = Map()
  def get_source_sas_id(journal : String, year : String, volume : String, number : String): String =
    var id: String = null

    val cache_key = s"$journal$year$volume$number"
    if (get_source_sas_id_cache.contains(cache_key)) {
      id = get_source_sas_id_cache(cache_key)
    } else {
      var sql_volume = ""
      if (volume.nonEmpty) {
        sql_volume = s"a.volume_serial = '$volume' AND "
      }

      var sql_number = ""
      if (number.nonEmpty) {
        sql_number = s"a.issue_number = '$number' AND "
      }

      val sql = s"""
        SELECT
          b.id as code
        FROM 
          biblioref_referencesource AS a, 
          biblioref_reference AS b 
        WHERE 
          a.title_serial = ? AND 
          a.reference_ptr_id = b.id AND
          $sql_volume
          $sql_number
          LEFT(b.publication_date_normalized,4) = ?
        ORDER BY
          CASE 
            WHEN b.status = -1 THEN -4
            ELSE b.status
          END DESC
        LIMIT 1
      """
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, journal)
      statement.setString(2, year)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          id = rs.getString("code")
        }
        get_source_sas_id_cache += cache_key -> id
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return id

  /**
    * Queries the Fi-Admin MySQL database for the Source ID 
    * for a MNT document.
    * Status priority = 1, 0, -3, -1
    * Each request is cached for optimization.
    */
  var get_source_mnt_id_cache: Map[String, String] = Map()
  def get_source_mnt_id(title : String, year : String, literature : String): String =
    var id: String = null

    val cache_key = s"$title$year$literature"
    if (get_source_mnt_id_cache.contains(cache_key)) {
      id = get_source_mnt_id_cache(cache_key)
    } else {
      val sql = """
        SELECT 
          b.id as code 
        FROM 
          biblioref_referencesource AS a, 
          biblioref_reference AS b 
        WHERE 
          b.literature_type = ? AND 
          NOT b.treatment_level LIKE 'am%' AND
          a.reference_ptr_id=b.id AND 
          a.title_monographic LIKE ? AND
          LEFT(b.publication_date_normalized,4) = ?
        ORDER BY
          CASE 
            WHEN b.status = -1 THEN -4
            ELSE b.status
          END DESC
        LIMIT 1
      """
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, literature)
      statement.setString(2, "%"+title+"%")
      statement.setString(3, year)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          id = rs.getString("code")
        }
        get_source_mnt_id_cache += cache_key -> id
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return id

  /**
    * Queries the Fi-Admin MySQL database for the title serial and ISSN.
    * Returns the title serial and ISSN as indexed in the database.
    * Each request is cached for optimization.
    */
  var get_title_and_issn_cache: Map[String, List[String]] = Map()
  def get_title_and_issn(title_serial: String, issn: String): List[String] =
    var result: List[String] = List("", "")

    val cache_key = s"$title_serial$issn"
    if (get_title_and_issn_cache.contains(cache_key)) {
      result = get_title_and_issn_cache(cache_key)
    } else {
      val sql = """
        SELECT 
          coalesce(NULLIF(a.shortened_title, ''), a.title) as 'title_serial',
          coalesce(NULLIF(a.issn, ''), c.issn) as 'issn',
          b.initial_date,
          b.initial_volume,
          b.initial_number,
          b.final_date,
          b.final_volume,
          b.final_number,
          b.indexer_cc_code
        FROM 
          title_title AS a,
          title_indexrange AS b,
          title_titlevariance AS c
        WHERE
          (
            ('' = ? OR a.shortened_title = ? OR a.title = ?) OR
            ('' = ? OR a.issn = ? OR c.issn = ?)
          ) AND
          c.title_id = a.id AND
          c.type = '240' AND 
          c.issn IS NOT NULL AND
          b.title_id = a.id AND 
          b.index_code_id = '17'
        LIMIT 1
      """
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, title_serial)
      statement.setString(2, title_serial)
      statement.setString(3, title_serial)
      statement.setString(4, issn)
      statement.setString(5, issn)
      statement.setString(6, issn)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          val title_serial_new = rs.getString("title_serial")
          val issn_new = rs.getString("issn")
          result = List(title_serial_new, issn_new)
        }
        get_title_and_issn_cache += cache_key -> result
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return result

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * Qualifier DeCS Code exists.
    * Each request is cached for optimization.
    *
    * @param qualifier
    * @return ID and DeCS qualifier version
    */
  var get_decs_qualifier_cache: Map[String, List[String]] = Map()
  def get_decs_qualifier(qualifier: String): List[String] =
    var result: List[String] = null

    if (get_decs_qualifier_cache.contains(qualifier)) {
      result = get_decs_qualifier_cache(qualifier)
    } else {
      val sql = """
          SELECT 
            a.`term_string`,
            c.`decs_code`
          FROM 
            `thesaurus_termlistqualif` AS a,
            `thesaurus_identifierconceptlistqualif` AS b,
            `thesaurus_identifierqualif` AS c 
          WHERE 
            a.`term_thesaurus` = '1' AND
            a.`concept_preferred_term` = 'Y' AND 
            a.`record_preferred_term`= 'Y' AND
            a.`identifier_concept_id` = b.`id` AND
            b.`preferred_concept`= 'Y' AND 
            b.`identifier_id`= c.id AND 
            LENGTH(c.`decs_code`) = '5' AND 
            a.`entry_version` = ?
          LIMIT 1
          """
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, qualifier)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          val id = "^s" + rs.getString("decs_code")
          val decs_qualifier = rs.getString("term_string")
          result = List(id, decs_qualifier)
        }
        get_decs_qualifier_cache += qualifier -> result
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return result

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * DeCS Descriptor Code exists.
    * Each request is cached for optimization.
    *
    * @param descriptor
    * @return ID and DeCS descriptor version
    */
  var get_decs_descriptor_cache: Map[String, String] = Map()
  def get_decs_descriptor(descriptor: String): String =
    var id: String = ""

    if (get_decs_descriptor_cache.contains(descriptor)) {
      id = get_decs_descriptor_cache(descriptor)
    } else {
      val sql = """
        SELECT 
          c.decs_code 
        FROM 
          thesaurus_termlistdesc AS a, 
          thesaurus_identifierconceptlistdesc AS b, 
          thesaurus_identifierdesc AS c 
        WHERE 
          a.term_thesaurus = '1' AND 
          a.concept_preferred_term = 'Y' AND 
          a.record_preferred_term= 'Y' AND 
          a.identifier_concept_id = b.id AND 
          b.preferred_concept= 'Y' AND 
          b.identifier_id= c.id AND 
          a.term_string = ?
        LIMIT 1
      """
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, descriptor)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          id = "^d" + rs.getString("decs_code")
        }
        get_decs_descriptor_cache += descriptor -> id
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return id

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * Country Code is valid.
    * Each request is cached for optimization.
    *
    * @param country_code
    * @return if the Country Code is valid
    */
  var is_country_code_valid_cache: Map[String, Boolean] = Map()
  def is_country_code_valid(country_code: String): Boolean =
    var is_country_valid: Boolean = false

    if (is_country_code_valid_cache.contains(country_code)) {
      is_country_valid = is_country_code_valid_cache(country_code)
    } else {
      val sql = "select id from utils_country where code=?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, country_code)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          is_country_valid = true
        }
        is_country_code_valid_cache += country_code -> is_country_valid
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return is_country_valid

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * Cooperative Center Code is valid.
    * Each request is cached for optimization.
    *
    * @param cc_code
    * @return if the Cooperative Center Code is valid
    */
  var is_cc_valid_cache: Map[String, Boolean] = Map()
  def is_cc_valid(cc_code: String): Boolean =
    var is_cc_valid: Boolean = false

    if (is_cc_valid_cache.contains(cc_code)) {
      is_cc_valid = is_cc_valid_cache(cc_code)
    } else {
      val sql = "select id from institution_institution where cc_code=?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, cc_code)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          is_cc_valid = true
        }
        is_cc_valid_cache += cc_code -> is_cc_valid
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return is_cc_valid

  /**
    * Queries the Fi-Admin MySQL database for evidence if the 
    * journal ISSN is indexed in LILACS or not.
    * Each request is cached for optimization.
    *
    * @param issn
    * @return if the journal ISSN is indexed in LILACS
    */
  var is_issn_lilacs_cache: Map[String, Boolean] = Map()
  def is_issn_lilacs(issn: String): Boolean =
    var is_issn_lilacs: Boolean = false

    if (is_issn_lilacs_cache.contains(issn)) {
      is_issn_lilacs = is_issn_lilacs_cache(issn)
    } else {
      val sql = "select title_title.id from title_title, title_indexrange where title_title.id=title_indexrange.title_id and title_indexrange.index_code_id=17 and issn=?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, issn)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          is_issn_lilacs = true
        }
        is_issn_lilacs_cache += issn -> is_issn_lilacs
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return is_issn_lilacs

  /**
    * Queries the Fi-Admin MySQL database for evidence if the shortened
    * version of a journal title is indexed in LILACS or not.
    * Each request is cached for optimization.
    *
    * @param journal_title
    * @return if the journal title is indexed in LILACS
    */
  var is_journal_title_lilacs_cache: Map[String, Boolean] = Map()
  def is_journal_title_lilacs(journal_title: String): Boolean =
    var is_journal_title_lilacs: Boolean = false

    if (is_journal_title_lilacs_cache.contains(journal_title)) {
      is_journal_title_lilacs = is_journal_title_lilacs_cache(journal_title)
    } else {
      val sql = "select title_title.id from title_title, title_indexrange where title_title.id=title_indexrange.title_id and title_indexrange.index_code_id=17 and shortened_title=?"
      val statement: PreparedStatement = connection.prepareStatement(sql)
      statement.setString(1, journal_title)

      try {
        val rs = statement.executeQuery
        while (rs.next) {
          is_journal_title_lilacs = true
        }
        is_journal_title_lilacs_cache += journal_title -> is_journal_title_lilacs
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return is_journal_title_lilacs

  /**
    * Closes the current MySQL connection
    */
  def close() =
    connection.close
    connection_source.close