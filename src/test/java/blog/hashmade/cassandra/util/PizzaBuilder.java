package blog.hashmade.cassandra.util;

import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

public final class PizzaBuilder {

  private static List<Pizza> PIZZAS = new LinkedList<Pizza>();
  static{
    Set<String> firstIngredients = new HashSet<String>();
    firstIngredients.add("ham");
    firstIngredients.add("cheese");

    Map<String, Double> firstMolecules = new HashMap<String, Double>();
    firstMolecules.put("proteins", 8.0);
    firstMolecules.put("glucids", 36.0);
    firstMolecules.put("lipids", 9.5);

    List<String> firstRecipiesSites = new LinkedList<String>();
    firstRecipiesSites.add("http://italian.food.com/recipe/ham-cheese-and-tomato-pizza-347450");
    firstRecipiesSites.add("http://www.boboli.com/BoboliRecipeDetail.aspx?id=2108");
    
    PIZZAS.add(new Pizza("ham and cheese", 200.0, firstIngredients, firstMolecules, firstRecipiesSites));
    
    Set<String> secondIngredients = new HashSet<String>();
    secondIngredients.add("ananas");

    Map<String, Double> secondMolecules = new HashMap<String, Double>();
    secondMolecules.put("proteins", 7.0);
    secondMolecules.put("glucids", 35.0);
    secondMolecules.put("lipids", 8.0);

    List<String> secondRecipiesSites = new LinkedList<String>();
    secondRecipiesSites.add("http://www.ptitchef.com/recettes/entree/pizza-crevettesananaschorizojambon-fid-1496963");
    
    PIZZAS.add(new Pizza("ananas", 180.0, secondIngredients, secondMolecules, secondRecipiesSites));
  }
  
  public static List<Pizza> getPizzas(){
    return PIZZAS;
  }
}
