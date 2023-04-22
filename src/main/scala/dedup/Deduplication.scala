package dedup

import java.nio.file.Paths
import java.io.File

import org.bireme.dcdup.DoubleCheckDuplicated
import dedup.PipeFile
import dedup.OutParser


/**
  * Manages the Dedup process.
  * It will call for pile files creation, will run 
  * BIREME's DoubleCheckDuplicated on each pipe file
  * then will parse the output to flag the duplicate records
  * in MongoDB
  */
class Deduplication():
  val charset = "UTF-8"

  val project_path = Paths.get(".").toAbsolutePath
  val dedup_conf_path = s"$project_path/data/Dedup"
  val dedup_io_path = s"$project_path/temp"

  // DeDup piped input file
  val in_pipe_path = s"$dedup_io_path/in.pipe"

  // Config Sas Five fields
  val conf_sas_five_path = s"$dedup_conf_path/configs/configLILACS_Sas_Five.cfg"
  // Config Sas Seven fields
  val conf_sas_seven_path = s"$dedup_conf_path/configs/configLILACS_Sas_Seven.cfg"
  // Config Mnt Four fields
  val conf_mnt_path = s"$dedup_conf_path/configs/configLILACS_MNT_Four.cfg"
  // Config Mntam Five fields
  val conf_mntam_path = s"$dedup_conf_path/configs/configLILACS_MNTam_Five.cfg"

  // Index Sas
  val index_sas_path = s"$dedup_conf_path/indexes/lilacs_Sas"
  // Index Mnt
  val index_mnt_path = s"$dedup_conf_path/indexes/lilacs_MNT"
  // Index Mntam
  val index_mntam_path = s"$dedup_conf_path/indexes/lilacs_MNTam"

  // duplicated records found in pipe file
  val out_dup1_path = s"$dedup_io_path/out_dup1"
  // duplicated records found between pipe file and DeDup index
  val out_dup2_path = s"$dedup_io_path/out_dup2"
  // no duplicated records between input pipe file and Dedup index
  val out_nodup1_path = s"$dedup_io_path/out_nodup1"
  // no duplicated records between (pipe file and itself) and (pipe file and Dedup index)
  val out_nodup2_path = s"$dedup_io_path/out_nodup2"

  override def toString: String = in_pipe_path

  def erase_in_out_files() =
    new File(in_pipe_path).delete()
    new File(out_dup1_path).delete()
    new File(out_dup2_path).delete()
    new File(out_nodup1_path).delete()
    new File(out_nodup2_path).delete()

  def run() =
    // MNT
    erase_in_out_files()
    PipeFile.create_mnt_pipe(in_pipe_path)
    run_dedup(index_mnt_path, conf_mnt_path)
    OutParser.parse_mnt(out_dup2_path, true)
    OutParser.parse_mnt(out_dup1_path, false)

    // MNTAM
    erase_in_out_files()
    PipeFile.create_mntam_pipe(in_pipe_path)
    run_dedup(index_mntam_path, conf_mntam_path)
    OutParser.parse_mntam(out_dup2_path, true)
    OutParser.parse_mntam(out_dup1_path, false)

    // SAS 7
    erase_in_out_files()
    PipeFile.create_sas_seven_pipe(in_pipe_path)
    run_dedup(index_sas_path, conf_sas_seven_path)
    OutParser.parse_sas7(out_dup2_path, true)
    OutParser.parse_sas7(out_dup1_path, false)

    // SAS 5
    erase_in_out_files()
    PipeFile.create_sas_five_pipe(in_pipe_path)
    run_dedup(index_sas_path, conf_sas_five_path)
    OutParser.parse_sas5(out_dup2_path, true)
    OutParser.parse_sas5(out_dup1_path, false)

  def run_dedup(index_path : String, conf_path : String) =
    DoubleCheckDuplicated.doubleCheck(
      in_pipe_path,
      charset,
      index_path,
      conf_path,
      charset,
      out_dup1_path,
      out_dup2_path,
      out_nodup1_path,
      out_nodup2_path
    )