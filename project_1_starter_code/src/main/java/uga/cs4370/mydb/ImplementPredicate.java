package uga.cs4370.mydb;

import java.util.List;

public class ImplementPredicate implements Predicate {

    private final String attr1;
    private final String attr2;
    private final String operator;

    public ImplementPredicate(String attr1, String attr2, String operator) {
        this.attr1 = attr1;
        this.attr2 = attr2;
        this.operator = operator;
    }

    @Override
    public boolean check(List<Cell> row) {
        // This method can be left unimplemented or used for single-row checks if needed
        return false;
    }

    public boolean evaluate(List<Cell> row1, List<Cell> row2) {
        Cell cell1 = null;
        Cell cell2 = null;

        // Find the cells corresponding to attr1 and attr2
        for (Cell cell : row1) {
            if (cell.getAsString().equals(attr1)) {
                cell1 = cell;
                break;
            }
        }

        for (Cell cell : row2) {
            if (cell.getAsString().equals(attr2)) {
                cell2 = cell;
                break;
            }
        }

        if (cell1 == null || cell2 == null) {
            throw new IllegalArgumentException("Attribute not found in rows");
        }

        // Evaluate the predicate
        switch (operator) {
            case "=":
                return cell1.getAsString().equals(cell2.getAsString());
            case "!=":
                return !cell1.getAsString().equals(cell2.getAsString());
            case "<":
                return ((Comparable) cell1.getAsInt()).compareTo(cell2.getAsInt()) < 0;
            case ">":
                return ((Comparable) cell1.getAsInt()).compareTo(cell2.getAsInt()) > 0;
            case "<=":
                return ((Comparable) cell1.getAsInt()).compareTo(cell2.getAsInt()) <= 0;
            case ">=":
                return ((Comparable) cell1.getAsInt()).compareTo(cell2.getAsInt()) >= 0;
            default:
                throw new IllegalArgumentException("Invalid operator: " + operator);
        }
    }
}
