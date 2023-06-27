import isis.IsisDB
import transform.Transformer
import dedup.Deduplication
import exporter.JsonExport


@main def main =  
  val database = IsisDB()
  database.import_data()

  Transformer.transform_docs()

  Deduplication().run()

  JsonExport.run()