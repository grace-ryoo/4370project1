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
       // How many students taking credits more than 20  
        Relation student = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();

        // dummy data 
        student.insert(List.of(Cell.val("S001"), Cell.val("John"), Cell.val("CS"), Cell.val(30)));
        student.insert(List.of(Cell.val("S002"), Cell.val("Jane"), Cell.val("Math"), Cell.val(20)));

        student.print();
        //  // predidate implement through anaonymous class
        Predicate creditPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(3).getAsInt() > 20;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

/**
        RAImpl raImpl = new RAImpl();

        Relation selectedStudents = raImpl.select(student, creditPredicate);

        System.out.println("\nSelected Students (tot_cred > 20):");
        selectedStudents.print();

        List<String> origAttrs = List.of("name", "dept_name");
        List<String> renamedAttrs = List.of("student_name", "department");

        RAImpl raImpl = new RAImpl();
        Relation renamed = raImpl.rename(student, origAttrs, renamedAttrs);
        renamed.print(); 
*/
        // Test cartesianProduct()
        /**
        Relation student = new RelationBuilder()
            .attributeNames(List.of("ID", "name"))
            .attributeTypes(List.of(Type.STRING, Type.STRING))
            .build();
        
        student.insert(List.of(Cell.val("S001"), Cell.val("John")));
        student.insert(List.of(Cell.val("S002"), Cell.val("Jane")));

        Relation course = new RelationBuilder()
            .attributeNames(List.of("course_id", "course_name"))
            .attributeTypes(List.of(Type.STRING, Type.STRING))
            .build();

        course.insert(List.of(Cell.val("C101"), Cell.val("Math")));
        course.insert(List.of(Cell.val("C102"), Cell.val("CS")));
       
        System.out.println("Student Relation:");
        student.print();
        
        System.out.println("Course Relation:");
        course.print();

        RAImpl raImpl = new RAImpl();
        Relation result = raImpl.cartesianProduct(student, course);

        // Print result relation
        System.out.println("Cartesian Product Result:");
        result.print();
*/

    }

}
