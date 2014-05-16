package akka.persistence.snapshot.cassandra

import java.lang.{ Long => JLong }
import java.nio.ByteBuffer

import scala.collection.JavaConverters._
import scala.collection.immutable.Seq
import scala.concurrent._

import akka.persistence.journal.AsyncWriteJournal
import akka.persistence._
import akka.serialization.SerializationExtension

import com.datastax.driver.core._
import com.datastax.driver.core.utils.Bytes
import akka.persistence.snapshot.SnapshotStore
import akka.persistence.serialization.Snapshot

class CassandraSnapshot extends SnapshotStore with CassandraStatements {
  val config = context.system.settings.config.getConfig("cassandra-snapshot")
  val extension = Persistence(context.system)

  val keyspace = config.getString("keyspace")
  val table = config.getString("table")

  val maxResultSize = config.getInt("max-result-size")

  val serialization = SerializationExtension(context.system)

  val cluster = Cluster.builder.addContactPoints(config.getStringList("contact-points").asScala: _*).build
  val session = cluster.connect()

  session.execute(createKeyspace(config.getInt("replication-factor")))
  session.execute(createTable)

  val writeConsistency = ConsistencyLevel.valueOf(config.getString("write-consistency"))
  val readConsistency = ConsistencyLevel.valueOf(config.getString("read-consistency"))

  val preparedWriteSnapshot = session.prepare(writeSnapshot).setConsistencyLevel(writeConsistency)
  val preparedDeleteSnapshot = session.prepare(deleteSnapshot).setConsistencyLevel(writeConsistency)
  val preparedLoadSnapshot = session.prepare(loadSnapshot).setConsistencyLevel(readConsistency)

  def snapshotToByteBuffer(p: Snapshot): ByteBuffer =
    ByteBuffer.wrap(serialization.serialize(p).get)

  def snapshotFromByteBuffer(b: ByteBuffer): Snapshot = {
    serialization.deserialize(Bytes.getArray(b), classOf[Snapshot]).get
  }

  override def postStop(): Unit = {
    session.close()
    cluster.close()
  }

  override def loadAsync(processorId: String, criteria: SnapshotSelectionCriteria): Future[Option[SelectedSnapshot]] = ??? //{
    //session.executeAsync(preparedLoadSnapshot.bind(processorId, criteria.maxSequenceNr, criteria.maxTimestamp)).map(_ m)
  //}

  override def saveAsync(metadata: SnapshotMetadata, snapshot: Any): Future[Unit] = {
    session.executeAsync(preparedWriteSnapshot.bind(metadata.processorId, metadata.sequenceNr, metadata.timestamp, snapshotToByteBuffer(Snapshot(snapshot)))).map(_ => ())
  }

  override def saved(metadata: SnapshotMetadata): Unit = ???

  override def delete(metadata: SnapshotMetadata): Unit = {
    session.execute(preparedDeleteSnapshot.bind(metadata.processorId, metadata.sequenceNr, metadata.timestamp))
  }

  override def delete(processorId: String, criteria: SnapshotSelectionCriteria): Unit = ???
}
