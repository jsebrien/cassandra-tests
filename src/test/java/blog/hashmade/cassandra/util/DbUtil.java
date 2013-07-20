package blog.hashmade.cassandra.util;

import java.util.concurrent.TimeUnit;

import org.apache.log4j.Logger;
import org.junit.Assert;

import blog.hashmade.cassandra.CollectionMapTest;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ExecutionInfo;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Select;

public class DbUtil {

  public static String listAll(Session session) throws Exception {
    DbUtil.LOGGER.info("------------------------------------------------------------------ ");
    Select selectStatement = QueryBuilder.select()
      .column("pizza_id")
      .column("name")
      .column("calories")
      .column("ingredients")
      .column("molecules")
      .column("recipies_sites")
      .from("pizzas")
      .limit(1)
      ;
    selectStatement.setConsistencyLevel(ConsistencyLevel.ONE).enableTracing();
    
    ResultSetFuture resultSetFuture = session.executeAsync(selectStatement.getQueryString());
    ResultSet resultSet = resultSetFuture.getUninterruptibly(DbUtil.DEFAULT_TIMEOUT_DURATION, DbUtil.DEFAULT_TIMEOUT_UNIT);
    ExecutionInfo info = resultSet.getExecutionInfo();
    Assert.assertNotNull(info);
    Row row = resultSet.one();
    String pizzaId = null;
    if (row != null) {
      pizzaId = row.getString("pizza_id");
      DbUtil.LOGGER.info("Id: " + pizzaId);
      DbUtil.LOGGER.info("Name: " + row.getString("name"));
      DbUtil.LOGGER.info("Calories: " + row.getDecimal("calories"));
      DbUtil.LOGGER.info("Ingredients: " + row.getSet("ingredients", String.class));
      DbUtil.LOGGER.info("Recipies Sites: " + row.getList("recipies_sites", String.class));
      DbUtil.LOGGER.info("Molecules: " + row.getMap("molecules", String.class, Double.class));
    }else{
      DbUtil.LOGGER.info("No pizza available");
    }
    DbUtil.LOGGER.info("------------------------------------------------------------------ ");
    return pizzaId;
  }

  public static final Logger LOGGER = Logger.getLogger(CollectionMapTest.class);
  public static final long DEFAULT_TIMEOUT_DURATION = 2L;
  public static final TimeUnit DEFAULT_TIMEOUT_UNIT = TimeUnit.SECONDS;

}
