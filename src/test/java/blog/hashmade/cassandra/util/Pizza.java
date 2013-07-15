package blog.hashmade.cassandra.util;

import java.util.List;
import java.util.Map;
import java.util.Set;

public class Pizza {

    private final String name;
    private final double nbCalories;
    private final Set<String> ingredients;
    private final Map<String, Double> molecules;
    private final List<String> recipiesSites;
    
    protected Pizza(
      String name,
      double nbCalories,
      Set<String> ingredients,
      Map<String, Double> molecules,
      List<String> recipiesSites) {
      super();
      this.name = name;
      this.nbCalories = nbCalories;
      this.ingredients = ingredients;
      this.molecules = molecules;
      this.recipiesSites = recipiesSites;
    }

    public String getName() {
      return name;
    }

    public double getNbCalories() {
      return nbCalories;
    }

    public Set<String> getIngredients() {
      return ingredients;
    }

    public Map<String, Double> getMolecules() {
      return molecules;
    }

    public List<String> getRecipiesSites() {
      return recipiesSites;
    }
}
