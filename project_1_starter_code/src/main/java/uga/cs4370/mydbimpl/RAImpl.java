package uga.cs4370.mydbimpl;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import uga.cs4370.mydb.Cell;
import uga.cs4370.mydb.Predicate;
import uga.cs4370.mydb.RA;
import uga.cs4370.mydb.Relation;
import uga.cs4370.mydb.RelationBuilder;
import uga.cs4370.mydb.Type;


public class RAImpl implements RA {

    /**
     * Performs the select operation on the relation rel by applying the
     * predicate p.
     *
     * @return The resulting relation after applying the select operation.
     */
    @Override
    public Relation select(Relation rel, Predicate p) {
        Relation selectedRelation = new RelationBuilder()
            .attributeNames(rel.getAttrs()) 
            .attributeTypes(rel.getTypes()) 
            .build();

        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> row = rel.getRow(i);
            if (p.check(row)) {
                selectedRelation.insert(row);
            }
        }
        return selectedRelation;
    }

    /**
     * Performs the project operation on the relation rel given the attributes
     * list attrs.
     *
     * @return The resulting relation after applying the project operation.
     *
     * @throws IllegalArgumentException If attributes in attrs are not present
     * in rel.
     */
    @Override
    public Relation project(Relation rel, List<String> attrs) {
        List<String> relAttrs = rel.getAttrs();
        List<Type> relTypes = rel.getTypes();

        // base case: check if attributes present n rel
        for (String attr : attrs) {
            if (!relAttrs.contains(attr)) {
                throw new IllegalArgumentException("Attribute not found in relation.");
            }
        }

        // find indices and types of attributes
        List<Integer> attrIndex = new ArrayList<>();
        List<Type> projectTypes = new ArrayList<>();
        for (String attr : attrs) {
            int index = relAttrs.indexOf(attr);
            attrIndex.add(index);
            projectTypes.add(relTypes.get(index));
        }

        // build new relation with the proper attributes
        Relation projectedRelation = new RelationBuilder()
            .attributeNames(attrs)
            .attributeTypes(projectTypes)
            .build();

        // use set to make sure there are no duplicates
        Set<List<Cell>> uniqueRows = new HashSet<>();

        // create new relation with projected data
        for (int i = 0; i < rel.getSize(); i++) {
            List<Cell> originalRow = rel.getRow(i);
            List<Cell> newRow = new ArrayList<>();
            for (int index : attrIndex) {
                newRow.add(originalRow.get(index));
            }
            uniqueRows.add(newRow);
        }

        // insert uniqueRows (no duplicates) into the new projected relation that was created
        for (List<Cell> row : uniqueRows) {
            projectedRelation.insert(row);
        }

        return projectedRelation;
    }


    /**
     * Performs the union operation on the relations rel1 and rel2.
     *
     * @return The resulting relation after applying the union operation.
     *
     * @throws IllegalArgumentException If rel1 and rel2 are not compatible.
     */
    @Override
    public Relation union(Relation rel1, Relation rel2) {
        return null;
    }

    /**
     * Performs the set difference operation on the relations rel1 and rel2.
     *
     * @return The resulting relation after applying the set difference
     * operation.
     *
     * @throws IllegalArgumentException If rel1 and rel2 are not compatible.
     */
    @Override
    public Relation diff(Relation rel1, Relation rel2) {
        return null;
    }

    /**
     * Renames the attributes in origAttr of relation rel to corresponding names
     * in renamedAttr.
     *
     * @return The resulting relation after renaming the attributes.
     *
     * @throws IllegalArgumentException If attributes in origAttr are not
     * present in rel or origAttr and renamedAttr do not have matching argument
     * counts.
     */
    @Override
    public Relation rename(Relation rel, List<String> origAttr, List<String> renamedAttr) {
        return null;
    }

    /**
     * Performs cartesian product on relations rel1 and rel2.
     *
     * @return The resulting relation after applying cartesian product.
     *
     * @throws IllegalArgumentException if rel1 and rel2 have common attributes.
     */
    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        return null;
    }

    /**
     * Performs natural join on relations rel1 and rel2.
     *
     * @return The resulting relation after applying natural join.
     */
    @Override
    public Relation join(Relation rel1, Relation rel2) {
        return null;
    }

    /**
     * Performs theta join on relations rel1 and rel2 with predicate p.
     *
     * @return The resulting relation after applying theta join. The resulting
     * relation should have the attributes of both rel1 and rel2. The attributes
     * of rel1 should appear in the the order they appear in rel1 but before the
     * attributes of rel2. Attributes of rel2 as well should appear in the order
     * they appear in rel2.
     *
     * @throws IllegalArgumentException if rel1 and rel2 have common attributes.
     */
    @Override
    public Relation join(Relation rel1, Relation rel2, Predicate p) {
        return null;
    }
}
