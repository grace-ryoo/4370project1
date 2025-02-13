package uga.cs4370.mydbimpl;

import uga.cs4370.mydb.*;
import java.util.List;
import java.util.ArrayList;

public class RAImpl implements RA {

    /**
     * Performs the select operation on the relation rel by applying the
     * predicate p.
     *
     * @return The resulting relation after applying the select operation.
     */
    @Override
    public Relation select(Relation rel, Predicate p) {

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

    }

    /**
     * Performs natural join on relations rel1 and rel2.
     *
     * @return The resulting relation after applying natural join.
     */
    @Override
    public Relation join(Relation rel1, Relation rel2) {
        // Get the attributes of the two relations
        List<String> attr1 = rel1.getAttrs();
        List<String> attr2 = rel2.getAttrs();

        // Find common attributes
        List<String> commonAttrs = new ArrayList<>();
        for (String attr : attr1) {
            if (attr2.contains(attr)) {
                commonAttrs.add(attr);
            }
        }

        // Create a new list of attributes for the resulting relation
        List<String> newAttrs = new ArrayList<>(attr1);
        for (String attr : attr2) {
            if (!commonAttrs.contains(attr)) {
                newAttrs.add(attr);
            }
        }

        // Create a new relation for the result
        RelationBuilder rb = new RelationBuilder();
        rb.attributeNames(newAttrs);
        Relation newRel = rb.build();

        // Perform the natural join
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);
                boolean match = true;
                for (String a : commonAttrs) {
                    if (!row1.get(attr1.indexOf(a)).getAsString().equals(row2.get(attr2.indexOf(a)).getAsString())) {
                        match = false;
                        break;
                    }
                }
                if (match) {
                    List<Cell> newRow = new ArrayList<>();
                    for (String a : newAttrs) {
                        if (attr1.contains(a)) {
                            newRow.add(row1.get(attr1.indexOf(a)));
                        } else {
                            newRow.add(row2.get(attr2.indexOf(a)));
                        }
                    }
                    newRel.insert(newRow);
                }
            }
        }

        return newRel;
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
        // Get the attributes of the two relations
        List<String> attr1 = rel1.getAttrs();
        List<String> attr2 = rel2.getAttrs();

        // Check for common attributes and throw an exception if any are found
        for (String attr : attr1) {
            if (attr2.contains(attr)) {
                throw new IllegalArgumentException("Relations have common attributes: " + attr);
            }
        }

        // Create a new list of attributes for the resulting relation
        List<String> newAttrs = new ArrayList<>(attr1);
        newAttrs.addAll(attr2);

        // Create a new relation builder for the result
        RelationBuilder rb = new RelationBuilder();
        rb.attributeNames(newAttrs);
        Relation newRel = rb.build();

        // Perform the theta join
        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> row1 = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> row2 = rel2.getRow(j);
                if (p.evaluate(row1, row2)) {
                    List<Cell> newRow = new ArrayList<>(row1);
                    newRow.addAll(row2);
                    newRel.insert(newRow); 
                }
            }
        }

        return newRel;
    }
}
