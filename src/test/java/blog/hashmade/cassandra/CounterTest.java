package blog.hashmade.cassandra;

import java.math.BigDecimal;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import blog.hashmade.cassandra.util.DbUtil;
import blog.hashmade.cassandra.util.Pizza;
import blog.hashmade.cassandra.util.PizzaBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
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
  public void testBound() throws Exception {
    ResultSet resultSet = null;
    ExecutionInfo info = null;
    
    Session session = cluster.connect("pizzastore");
    
    LOGGER.info("Insert 1 entry");
    /*Insert insertStatement = QueryBuilder.insertInto("stats");
    insertStatement.value("pizza_id", UUIDs.random().toString());
    
    resultSet = session.execute(insertStatement.getQueryString());
    info = resultSet.getExecutionInfo();*/
    
    String pizzaId = UUIDs.random().toString();
    
    LOGGER.info("Increment the counter");
    // Update 1 pizza
    Update updateStatement = QueryBuilder.update("stats");
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));
    updateStatement.with(QueryBuilder.incr("nb_ordered"));

    resultSet = session.execute(updateStatement.getQueryString());
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    LOGGER.info("Decrement the counter");
    // Update 1 pizza
    updateStatement = QueryBuilder.update("stats");
    updateStatement.with(QueryBuilder.decr("nb_ordered"));
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSet = session.execute(updateStatement.getQueryString());
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    LOGGER.info("Delete the counter");
    Delete.Selection delSomeSelection = QueryBuilder.delete()
      .column("nb_ordered");
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

  @After
  public void closeAll() throws Exception {
    cluster.shutdown();
  }
}
