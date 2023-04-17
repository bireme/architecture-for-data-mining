package mysql
import config.Settings
import java.sql.{Connection,DriverManager}


/**
  * An abstraction to handle Fi-Admin data in MySQL
  */
object Fiadmin:
  val url = Settings.getConf("FIADMIN_MYSQL_URL")
  val driver = "com.mysql.cj.jdbc.Driver"
  val username = Settings.getConf("FIADMIN_MYSQL_USER")
  val password = Settings.getConf("FIADMIN_MYSQL_PASSWORD")
  var connection:Connection = _

  try {
    Class.forName(driver)
    connection = DriverManager.getConnection(url, username, password)
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

    if (this.database_id_cache.contains(database)) {
      id = this.database_id_cache(database)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"select id from database_database where acronym = '$database'"
        )
        while (rs.next) {
          id = rs.getString("id")
        }
        this.database_id_cache += database -> id
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
    if (this.is_code_valid_cache.contains(cache_key)) {
      is_code_valid = this.is_code_valid_cache(cache_key)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"SELECT id FROM utils_auxcode WHERE field='$field' and code='$code'"
        )
        while (rs.next) {
          is_code_valid = true
        }
        this.is_code_valid_cache += cache_key -> is_code_valid
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

    if (this.get_country_code_cache.contains(country)) {
      id = this.get_country_code_cache(country)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"select code from utils_country left join utils_countrylocal on utils_country.id=country_id where utils_country.name='$country' OR utils_countrylocal.name='$country'"
        )
        while (rs.next) {
          id = rs.getString("code")
        }
        this.get_country_code_cache += country -> id
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
    if (this.get_title_and_issn_cache.contains(cache_key)) {
      result = this.get_title_and_issn_cache(cache_key)
    } else {
      var sql_title = ""
      var sql_issn = ""
      if (title_serial != "") {
        sql_title = s"(a.shortened_title = '$title_serial' OR a.title = '$title_serial')"
      }
      if (issn != "") {
        sql_issn = s"(a.issn = '$issn' OR c.issn = '$issn')"
        if (sql_title != "") {
          sql_issn = s" OR $sql_issn"
        }
      }

      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"""
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
            ($sql_title $sql_issn) AND
            c.title_id = a.id AND
            c.type = '240' AND 
            c.issn IS NOT NULL AND
            b.title_id = a.id AND 
            b.index_code_id = '17'
          LIMIT 1
          """
        )
        while (rs.next) {
          val title_serial_new = rs.getString("title_serial")
          val issn_new = rs.getString("issn")
          result = List(title_serial_new, issn_new)
        }
        this.get_title_and_issn_cache += cache_key -> result
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

    if (this.get_decs_qualifier_cache.contains(qualifier)) {
      result = this.get_decs_qualifier_cache(qualifier)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"""
          SELECT 
            a.`term_string`,
            c.`decs_code`
          FROM 
            `thesaurus_termlistqualif` AS a,
            `thesaurus_identifierconceptlistqualif` AS b,
            `thesaurus_identifierqualif` AS c 
          WHERE 
            a.`language_code` = 'ES' AND 
            a.`term_thesaurus` = '1' AND
            a.`concept_preferred_term` = 'Y' AND 
            a.`record_preferred_term`= 'Y' AND
            a.`identifier_concept_id` = b.`id` AND
            b.`preferred_concept`= 'Y' AND 
            b.`identifier_id`= c.id AND 
            LENGTH(c.`decs_code`) = '5' AND 
            a.`entry_version` = '$qualifier'
          LIMIT 1
          """
        )
        while (rs.next) {
          val id = "^s" + rs.getString("decs_code")
          val decs_qualifier = rs.getString("term_string")
          result = List(id, decs_qualifier)
        }
        this.get_decs_qualifier_cache += qualifier -> result
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

    if (this.get_decs_descriptor_cache.contains(descriptor)) {
      id = this.get_decs_descriptor_cache(descriptor)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"""
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
            a.term_string = '$descriptor'
          LIMIT 1
          """
        )
        while (rs.next) {
          val id = "^d" + rs.getString("decs_code")
        }
        this.get_decs_descriptor_cache += descriptor -> id
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

    if (this.is_country_code_valid_cache.contains(country_code)) {
      is_country_valid = this.is_country_code_valid_cache(country_code)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"select id from utils_country where code='$country_code'"
        )
        while (rs.next) {
          is_country_valid = true
        }
        this.is_country_code_valid_cache += country_code -> is_country_valid
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

    if (this.is_cc_valid_cache.contains(cc_code)) {
      is_cc_valid = this.is_cc_valid_cache(cc_code)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"select id from institution_institution where cc_code='$cc_code'"
        )
        while (rs.next) {
          is_cc_valid = true
        }
        this.is_cc_valid_cache += cc_code -> is_cc_valid
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

    if (this.is_issn_lilacs_cache.contains(issn)) {
      is_issn_lilacs = this.is_issn_lilacs_cache(issn)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"select title_title.id from title_title, title_indexrange where title_title.id=title_indexrange.title_id and title_indexrange.index_code_id=17 and issn='$issn'"
        )
        while (rs.next) {
          is_issn_lilacs = true
        }
        this.is_issn_lilacs_cache += issn -> is_issn_lilacs
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

    if (this.is_journal_title_lilacs_cache.contains(journal_title)) {
      is_journal_title_lilacs = this.is_journal_title_lilacs_cache(journal_title)
    } else {
      try {
        val statement = this.connection.createStatement
        val rs = statement.executeQuery(
          s"select title_title.id from title_title, title_indexrange where title_title.id=title_indexrange.title_id and title_indexrange.index_code_id=17 and shortened_title='$journal_title'"
        )
        while (rs.next) {
          is_journal_title_lilacs = true
        }
        this.is_journal_title_lilacs_cache += journal_title -> is_journal_title_lilacs
      } catch {
        case e: Exception => e.printStackTrace
      }
    }
    return is_journal_title_lilacs

  /**
    * Closes the current MySQL connection
    */
  def close() =
    this.connection.close