package uga.cs4370.mydbimpl;

import java.util.List;

import uga.cs4370.mydb.Cell;
import uga.cs4370.mydb.Predicate;
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

       // interesting query #1 - catherine
       // How many students taking credits more than 125  
        Relation student = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        student.loadData("project_1_starter_code/src/main/java/uga/cs4370/data/mysql-files/student.csv"); // this path should work but might have to change it to your personal absolute path 

        //  predidate implement through anaonymous class
        Predicate creditPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(3).getAsInt() > 125;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };


        RAImpl raImpl = new RAImpl();

        Relation selectedStudents = raImpl.select(student, creditPredicate);

        System.out.println("\nStudents taking tot_creds > 125: ");
        selectedStudents.print();
    }

}
