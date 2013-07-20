package blog.hashmade.cassandra;

import java.util.HashSet;
import java.util.Set;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import blog.hashmade.cassandra.util.DbUtil;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.Delete;
import com.datastax.driver.core.querybuilder.Insert;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update;
import com.datastax.driver.core.utils.UUIDs;

public class CollectionMapTest {

  private static final String FIRST_WEBSITE = "http://italian.food.com/recipe/ham-cheese-and-tomato-pizza-347450";
  private static final String SECOND_WEBSITE = "http://www.boboli.com/BoboliRecipeDetail.aspx?id=2108";

  private static final String NODE_IP = "127.0.0.1";

  private Cluster cluster;

  @Before
  public void setUp() throws Exception {
    cluster = Cluster.builder().addContactPoint(NODE_IP).build();
    Metadata metadata = cluster.getMetadata();
    DbUtil.LOGGER.info("Connected to cluster: " +  metadata.getClusterName());
  }

  @Test
  public void testCollections() throws Exception {
    ResultSetFuture resultSetFuture = null;
    ResultSet resultSet = null;
    ExecutionInfo info = null;
    Session session = cluster.connect("pizzastore");

    Set<String> ingredients = new HashSet<String>();
    ingredients.add("ham");
    ingredients.add("onion");
    ingredients.add("basil");

    // Insert 1 pizza
    DbUtil.LOGGER.info("Insert 1 pizza with name, calories and ingredients");
    Insert insertStatement = QueryBuilder.insertInto("pizzas");
    insertStatement.value("pizza_id", UUIDs.random().toString())
      .value("name", "ham and cheese")
      .value("calories", 254.0)
      .value("ingredients", ingredients);

    resultSetFuture = session.executeAsync(insertStatement.getQueryString());
    resultSet = resultSetFuture.getUninterruptibly(DbUtil.DEFAULT_TIMEOUT_DURATION, DbUtil.DEFAULT_TIMEOUT_UNIT);
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    // List the first pizza
    String pizzaId = DbUtil.listAll(session);

    DbUtil.LOGGER.info("Update this pizza with a molecules map, and a recipies_sites list");
    // Update 1 pizza
    Update updateStatement = QueryBuilder.update("pizzas");
    updateStatement.with(QueryBuilder.put("molecules", "proteins", 8.0));
    updateStatement.with(QueryBuilder.put("molecules", "glucids", 36.0));
    updateStatement.with(QueryBuilder.put("molecules", "lipids", 9.5));
    updateStatement.with(QueryBuilder.append("recipies_sites", FIRST_WEBSITE));
    updateStatement.with(QueryBuilder.append("recipies_sites", SECOND_WEBSITE));
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSetFuture = session.executeAsync(updateStatement.getQueryString());
    resultSet = resultSetFuture.getUninterruptibly(DbUtil.DEFAULT_TIMEOUT_DURATION, DbUtil.DEFAULT_TIMEOUT_UNIT);
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    // List the first pizza
    pizzaId = DbUtil.listAll(session);

    DbUtil.LOGGER.info("Update this pizza: change calories, add and remove ingredients, remove one recipies_sites");
    // Update 1 pizza
    updateStatement = QueryBuilder.update("pizzas");
    updateStatement.with(QueryBuilder.set("calories", 243.0)); // change calories value
    updateStatement.with(QueryBuilder.add("ingredients", "parmesan cheese")); // add "parmesan cheese" to ingredients set
    updateStatement.with(QueryBuilder.remove("ingredients", "ham")); // remove "ham" for ingredients set
    updateStatement.with(QueryBuilder.discard("recipies_sites", FIRST_WEBSITE)); // remove first website from recipies_sites list
    updateStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSetFuture = session.executeAsync(updateStatement.getQueryString());
    resultSet = resultSetFuture.getUninterruptibly(DbUtil.DEFAULT_TIMEOUT_DURATION, DbUtil.DEFAULT_TIMEOUT_UNIT);
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    // List the first pizza
    pizzaId = DbUtil.listAll(session);

    DbUtil.LOGGER.info("Change some of this pizza's fields: remove calories, remove one recipies_sites, remove one molecule in map");
    Delete.Selection delSomeSelection = QueryBuilder.delete()
      .column("calories")
      .listElt("recipies_sites", 0)
      .mapElt("molecules", "lipids");
    Delete deleteStatement = delSomeSelection.from("pizzas");
    deleteStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSetFuture = session.executeAsync(deleteStatement.getQueryString());
    resultSet = resultSetFuture.getUninterruptibly(DbUtil.DEFAULT_TIMEOUT_DURATION, DbUtil.DEFAULT_TIMEOUT_UNIT);
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    // List the first pizza
    pizzaId = DbUtil.listAll(session);

    DbUtil.LOGGER.info("Delete the pizza");
    // Delete the whole pizza
    deleteStatement = QueryBuilder.delete().from("pizzas");
    deleteStatement.where(QueryBuilder.eq("pizza_id", pizzaId));

    resultSetFuture = session.executeAsync(deleteStatement.getQueryString());
    resultSet = resultSetFuture.getUninterruptibly(DbUtil.DEFAULT_TIMEOUT_DURATION, DbUtil.DEFAULT_TIMEOUT_UNIT);
    info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);

    // List the first pizza
    DbUtil.listAll(session);
    session.shutdown();
  }

  @After
  public void closeAll() throws Exception {
    cluster.shutdown();
  }
}
