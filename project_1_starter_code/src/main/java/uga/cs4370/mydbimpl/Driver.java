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
       // How many students taking credits more than 125 & who are enrolled in at least one course.
        Relation student = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        student.loadData("mysql_exports/student.csv"); // this path should work but might have to change it to your personal absolute path 

        Relation takes = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year", "grade"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING))
                .build();
        takes.loadData("mysql_exports/takes.csv");

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

        Relation filteredStudents = raImpl.select(student, creditPredicate);
        Relation joinedStudents = raImpl.join(filteredStudents, takes);  // join on student ID

        Relation finalResult = raImpl.project(joinedStudents, List.of("ID", "name", "dept_name", "tot_cred", "course_id"));

        System.out.println("\nStudents taking tot_creds > 125 and enrolled in at least one course: ");
        finalResult.print();

        // interesting query #2 - Grace Ryoo
        // What are all possible classroom and time slot combinations on Monday ('M') with classroom capacities greater than 50 (classroom.capacity > 50)
        Relation classrooms = new RelationBuilder()
                .attributeNames(List.of("building", "room_number", "capacity"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        classrooms.loadData("mysql_exports/classrooms.csv"); // this path should work but might have to change it to your personal absolute path 

        Relation times = new RelationBuilder()
                .attributeNames(List.of("time_slot_id", "day", "start_hr", "start_min", "end_hr", "end_min"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.INTEGER, Type.INTEGER, Type.INTEGER, Type.INTEGER))
                .build();
        times.loadData("mysql_exports/times.csv");

        //  predidate checking for capacity > 50
        Predicate capacityPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(2).getAsInt() > 50;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        //  predidate filtering for Monday time slots
        Predicate mondayPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(1).getAsString().equals("M");
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        RAImpl raImpl2 = new RAImpl();

        Relation filteredClassrooms = raImpl2.select(classrooms, capacityPredicate);
        Relation filteredTimes = raImpl2.select(times, mondayPredicate);  // join on student ID

        Relation classroomTimeCombinations = raImpl2.cartesianProduct(filteredClassrooms, filteredTimes);

        Relation finalResult2 = raImpl2.project(classroomTimeCombinations, List.of("building", "room_number", "capacity", "time_slot_id", "start_hr", "start_min", "end_hr", "end_min"));

        System.out.println("\nAll possible classroom and time slot combinations on day 'M' with capacity > 50: ");
        finalResult2.print();


    /**

        List<String> origAttrs = List.of("name", "dept_name");
        List<String> renamedAttrs = List.of("student_name", "department");

        RAImpl raImpl = new RAImpl();
        Relation renamed = raImpl.rename(student, origAttrs, renamedAttrs);
        renamed.print(); 
**/
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
