package akka.persistence.snapshot.cassandra

trait CassandraStatements {
  def keyspace: String
  def table: String

  def createKeyspace(replicationFactor: Int) = s"""
      CREATE KEYSPACE IF NOT EXISTS ${keyspace}
      WITH REPLICATION = { 'class' : 'SimpleStrategy', 'replication_factor' : ${replicationFactor} }
    """

  def createTable = s"""
      CREATE TABLE IF NOT EXISTS ${tableName} (
        processor_id text,
        sequence_nr bigint,
        timestamp timestamp,
        snapshot blob,
        PRIMARY KEY (processor_id, sequence_nr, timestamp))
        WITH COMPACT STORAGE
    """

  def writeSnapshot= s"""
      INSERT INTO ${tableName} (processor_id, sequence_nr, timestamp, snapshot)
      VALUES (?, ?, ?, ?)
    """

  def deleteSnapshot= s"""
      DELETE FROM ${tableName} where processor_id = ? and sequence_nr = ? and timestamp = ?
    """

  def deleteSnapshots= s"""
      DELETE FROM ${tableName} where processor_id = ? and sequence_nr < ? and timestamp < ?
    """

  def loadSnapshot= s"""
      SELECT * FROM ${tableName} where processor_id = ? and sequence_nr < ? and timestamp < ? order by sequence_nr desc limit 1
     """

  private def tableName = s"${keyspace}.${table}"
}
