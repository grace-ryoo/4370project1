package uga.cs4370.mydbimpl;

import java.util.ArrayList;

import uga.cs4370.mydb.*;

import java.util.List;

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
        if (origAttr.size() != renamedAttr.size()) {
            throw new IllegalArgumentException("Attributes in origAttr and renamedAttr must have the same size.");
        } // if

        List<String> newAttrNamesList = new ArrayList<>(rel.getAttrs());

        for (int i = 0; i < origAttr.size(); i++) {
            String ogName = origAttr.get(i);
            String newName = renamedAttr.get(i);

            if (!rel.hasAttr(ogName)) {
                throw new IllegalArgumentException("Attribute " + ogName + " is not present in relation.");
            } // if

            int relIndex = rel.getAttrIndex(ogName);
            newAttrNamesList.set(relIndex, newName);
        } // for

        Relation renamedRelation = new RelationBuilder()
                .attributeNames(newAttrNamesList)
                .attributeTypes(rel.getTypes())
                .build();

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
            } // if
        } // for

        List<String> newAttrNames = new ArrayList<>(rel1Attr);
        newAttrNames.addAll(rel2Attr);

        List<Type> newAttrTypes = new ArrayList<>(rel1.getTypes());
        newAttrTypes.addAll(rel2.getTypes());

        Relation cpRelation = new RelationBuilder()
                .attributeNames(newAttrNames)
                .attributeTypes(newAttrTypes)
                .build();

        return cpRelation;
    } // cartesianProduct

    /**
     * Performs natural join on relations rel1 and rel2.
     *
     * @return The resulting relation after applying natural join.
     */
    @Override
    public Relation join(Relation rel1, Relation rel2) {

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

    }
}
