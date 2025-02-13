package uga.cs4370.mydbimpl;

import java.util.List;

import uga.cs4370.mydb.Cell;
import uga.cs4370.mydb.Relation;
import uga.cs4370.mydb.RelationBuilder;
import uga.cs4370.mydb.Type;

public class Driver {
    
    public static void main(String[] args) {
        // Following is an example of how to use the relation class.
        // This creates a table with three columns with below mentioned
        // column names and data types.
        // After creating the table, data is loaded from a CSV file.
        // Path should be replaced with a correct file path for a compatible
        // CSV file.
        /**
        Relation rel1 = new RelationBuilder()
                .attributeNames(List.of("Col01_Name", "Col02_Name", "Col03_Name"))
                .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.DOUBLE))
                .build();
        rel1.loadData("/path/to/exported/csv_file");
        rel1.print();
*/
        Relation student = new RelationBuilder()
            .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
            .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
            .build();

        student.insert(List.of(Cell.val("S001"), Cell.val("John"), Cell.val("CS"), Cell.val(30)));
        student.insert(List.of(Cell.val("S002"), Cell.val("Jane"), Cell.val("Math"), Cell.val(20)));

        student.print();

        List<String> og = List.of("John", "Jane");
        List<String> on = List.of("Ella", "Ally");
       

        RAImpl raImpl = new RAImpl();

        Relation renamedStudents = raImpl.rename(student, og, on);
        renamedStudents.print();
    }

}
