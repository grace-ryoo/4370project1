package uga.cs4370.mydbimpl;

import java.util.ArrayList;
import java.util.List;

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
        return null;
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
        return null;
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
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Attributes in origAttr and renamedAttr must have the same size.");
        }

        List<String> newAttrNamesList = new ArrayList<>(rel.getAttrs());

        for (int i = 0; i < origAttr.size(); i++) {
            String ogName = origAttr.get(i);
            String newName = renamedAttr.get(i);

            if (!rel.hasAttr(ogName)) {
                throw new IllegalArgumentException("Attribute " + ogName + " is not present in relation.");
            }

            int relIndex = rel.getAttrIndex(ogName);
            newAttrNamesList.set(relIndex, newName);
        }

        Relation renamedRelation = new RelationBuilder()
                .attributeNames(newAttrNamesList)
                .attributeTypes(rel.getTypes())
                .build();

        for (int j = 0; j < rel.getSize(); j++) {
            renamedRelation.insert(rel.getRow(j));
        }
        return renamedRelation;
    
    } // rename

    /**
     * Performs cartesian product on relations rel1 and rel2.
     *
     * @return The resulting relation after applying cartesian product.
     *
     * @throws IllegalArgumentException if rel1 and rel2 have common attributes.
     */
    @Override
    public Relation cartesianProduct(Relation rel1, Relation rel2) {
        List<String> rel1Attr = rel1.getAttrs();
        List<String> rel2Attr = rel2.getAttrs();

        for (String a : rel1Attr) {
            if (rel2.hasAttr(a)) {
                throw new IllegalArgumentException("Attributes in rel1 and rel2 have common attributes.");
            }
        }

        List<String> newAttrNames = new ArrayList<>(rel1Attr);
        newAttrNames.addAll(rel2Attr);

        List<Type> newAttrTypes = new ArrayList<>(rel1.getTypes());
        newAttrTypes.addAll(rel2.getTypes());

        Relation cpRelation = new RelationBuilder()
                .attributeNames(newAttrNames)
                .attributeTypes(newAttrTypes)
                .build();

        for (int i = 0; i < rel1.getSize(); i++) {
            List<Cell> rel1row = rel1.getRow(i);
            for (int j = 0; j < rel2.getSize(); j++) {
                List<Cell> rel2row = rel2.getRow(j);
                List<Cell> combinationRow = new ArrayList<>(rel1row);
                combinationRow.addAll(rel2row);
                cpRelation.insert(combinationRow);
            }
        }

        return cpRelation;
    } // cartesianProduct

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
