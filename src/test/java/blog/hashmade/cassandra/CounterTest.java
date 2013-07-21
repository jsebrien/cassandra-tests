package blog.hashmade.cassandra;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.utils.UUIDs;

public class CounterTest {

  private static final Logger LOGGER = Logger.getLogger(CollectionMapTest.class);

  private static final String NODE_IP = "127.0.0.1";

  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = Cluster.builder().addContactPoint(NODE_IP).build();
    Metadata metadata = cluster.getMetadata();
    LOGGER.info("Connected to cluster: " + metadata.getClusterName());
  }

  @Test
  public void testCounter() throws Exception {
    ResultSet resultSet = null;
    ExecutionInfo info = null;

    Session session = cluster.connect("pizzastore");

    String pizzaId = UUIDs.random().toString();

    LOGGER.info("Increment the counter");
    // Update 1 pizza
    Update updateStatement = QueryBuilder.update("stats");
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));
    updateStatement.with(QueryBuilder.incr("nb_ordered"));

    resultSet = session.execute(updateStatement.getQueryString());
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    printCounterValue(session, pizzaId);

    LOGGER.info("Decrement the counter");
    // Update 1 pizza
    updateStatement = QueryBuilder.update("stats");
    updateStatement.with(QueryBuilder.decr("nb_ordered"));
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSet = session.execute(updateStatement.getQueryString());
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);
    
    printCounterValue(session, pizzaId);
    
    updateStatement = QueryBuilder.update("stats");
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));
    updateStatement.with(QueryBuilder.incr("nb_ordered", 3));
    
    resultSet = session.execute(updateStatement.getQueryString());
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);
    
    printCounterValue(session, pizzaId);
    
    LOGGER.info("Delete the counter");
    Delete.Selection delSomeSelection = QueryBuilder.delete().column("nb_ordered");
    Delete deleteStatement = delSomeSelection.from("stats");
    deleteStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSet = session.execute(deleteStatement.getQueryString());
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    LOGGER.info("Try to recreate the counter");
    updateStatement = QueryBuilder.update("stats");
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));
    updateStatement.with(QueryBuilder.incr("nb_ordered"));

    resultSet = session.execute(updateStatement.getQueryString());

    session.shutdown();
  }

  public void printCounterValue(Session session, String pizzaId) {
    ResultSet resultSet;
    Select selectStatement = QueryBuilder.select()
      .column("nb_ordered")
      .from("stats")
      .where(QueryBuilder.eq("pizza_id", pizzaId))
      .limit(1);
    selectStatement.setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();

    resultSet = session.execute(selectStatement.getQueryString());
    Row row = resultSet.one();
    if (row != null) {
      LOGGER.info("Current counter value is: " + row.getLong("nb_ordered"));
    }
  }

  @After
  public void closeAll() throws Exception {
    cluster.shutdown();
  }
}
