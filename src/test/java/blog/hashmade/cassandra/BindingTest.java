package blog.hashmade.cassandra;

import java.math.BigDecimal;

import org.apache.log4j.Logger;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import blog.hashmade.cassandra.util.Pizza;
import blog.hashmade.cassandra.util.PizzaBuilder;

import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.utils.UUIDs;

public class BindingTest {

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

    Insert insertStatement = QueryBuilder.insertInto("pizzas");
    insertStatement.value("pizza_id", QueryBuilder.bindMarker())
      .value("name", QueryBuilder.bindMarker())
      .value("calories", QueryBuilder.bindMarker())
      .value("ingredients", QueryBuilder.bindMarker())
      .value("molecules", QueryBuilder.bindMarker())
      .value("recipies_sites", QueryBuilder.bindMarker());
    ;

    PreparedStatement preparedStatement = session.prepare(insertStatement.getQueryString());

    BoundStatement boundStatement = new BoundStatement(preparedStatement);
    for (Pizza pizza : PizzaBuilder.getPizzas()) {
      resultSet = session.execute(boundStatement.bind(
        UUIDs.random().toString(),
        pizza.getName(),
        new BigDecimal(pizza.getNbCalories()),
        pizza.getIngredients(),
        pizza.getMolecules(),
        pizza.getRecipiesSites()));

      info = resultSet.getExecutionInfo();
      Assert.assertNotNull(info);
    }
    session.shutdown();
  }

  @After
  public void closeAll() throws Exception {
    cluster.shutdown();
  }
    
}
