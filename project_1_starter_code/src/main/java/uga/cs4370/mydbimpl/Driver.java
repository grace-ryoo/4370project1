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
         * Relation rel1 = new RelationBuilder()
         * .attributeNames(List.of("Col01_Name", "Col02_Name", "Col03_Name"))
         * .attributeTypes(List.of(Type.INTEGER, Type.STRING, Type.DOUBLE))
         * .build(); rel1.loadData("/path/to/exported/csv_file"); rel1.print();
         */

        // interesting query #1 - catherine
        // How many students taking credits more than 125 & who are enrolled in at least one course and are in the Elec. Eng. Department.
        Relation student = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        student.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/student.csv"); // this path should work but might have to change it to your personal absolute path 

        Relation takes = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year", "grade"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING))
                .build();
        takes.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/takes.csv");

        Predicate creditPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(3).getAsInt() > 125 && row.get(2).getAsString().equals("Elec. Eng.");
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

        System.out.println("\nStudents taking tot_creds > 125 and enrolled in at least one course and are in the Elec. Eng. Department: ");
        finalResult.print();

        // interesting query #2 - Grace Ryoo
        // What are all possible classroom and time slot combinations on Monday ('M') with classroom capacities greater than 50 (classroom.capacity > 50)
        Relation classrooms = new RelationBuilder()
                .attributeNames(List.of("building", "room_number", "capacity"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        classrooms.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/classroom.csv");

        Relation times = new RelationBuilder()
                .attributeNames(List.of("time_slot_id", "day", "start_hr", "start_min", "end_hr", "end_min"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.INTEGER, Type.INTEGER, Type.INTEGER, Type.INTEGER))
                .build();
        times.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/time_slot.csv");

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

        // interesting query #3 - Khushi Bhatamrekar
        // find all instructors who teach courses in the "Comp. Sci." department, have a
        // salary greater than $80,000, and are teaching a course that has a
        // prerequisite
        Relation instructor = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "instructor_dept_name", "salary"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.DOUBLE))
                .build();
        instructor.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/instructor.csv"); // this path should work but might have to change it to your personal absolute path

        Relation course = new RelationBuilder()
                .attributeNames(List.of("course_id", "title", "dept_name", "credits"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        course.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/course.csv");

        Relation prereqs = new RelationBuilder()
                .attributeNames(List.of("prereq_course_id", "prereq_id"))
                .attributeTypes(List.of(Type.STRING, Type.STRING))
                .build();
        prereqs.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/prereq.csv");

        Predicate salaryPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(3).getAsDouble() > 80000;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        Predicate compSciPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(2).getAsString().equals("Comp. Sci.");
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        RAImpl raImpl3 = new RAImpl();
        Relation instructors = raImpl3.select(instructor, salaryPredicate);
        Relation compSciCourses = raImpl3.select(course, compSciPredicate);

        Relation courseJoin = raImpl3.join(instructors, compSciCourses, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return false;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return row1.get(2).getAsString().equals(row2.get(2).getAsString());
            }
        });

        Relation finalJoin = raImpl3.join(courseJoin, prereqs, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return false;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return row1.get(4).getAsString().equals(row2.get(0).getAsString());
            }
        });

        Relation rel3Result = raImpl3.project(finalJoin,
                List.of("ID", "name", "dept_name", "salary", "course_id", "title", "prereq_id"));

        System.out.println(
                "\nInstructors who teach courses in the 'Comp. Sci.' department, have a salary greater than $80,000, and are teaching a course with prerequisites: ");
        rel3Result.print();

        // interesting query #4 - ADITI
        // List of students who are enrolled in a "Comp. Sci." course and a "Math" course but never recieved an A in any course.
        Relation students = new RelationBuilder()
                .attributeNames(List.of("ID", "name", "dept_name", "tot_cred"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        students.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/student.csv");
        Relation takesCourse = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year", "grade"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING))
                .build();
        takesCourse.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/takes.csv");
        Relation courses = new RelationBuilder()
                .attributeNames(List.of("course_id", "title", "dept_name", "credits"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        courses.loadData("4370project1/project_1_starter_code/target/classes/uga/cs4370/data/mysql-files/course.csv");

        //predicates
        Predicate compSciCoursePredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(2).getAsString().equals("Comp. Sci.");
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };
        Predicate mathCoursePredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(2).getAsString().equals("Math");
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };
        Predicate gradeAPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return row.get(5).getAsString().equals("A");
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        RAImpl raImpl4 = new RAImpl();
        Relation compSci = raImpl4.select(courses, compSciCoursePredicate);
        Relation mathCourses = raImpl4.select(courses, mathCoursePredicate);
        Relation studentsInCompSci = raImpl4.join(takesCourse, compSci, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return false;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return row1.get(1).getAsString().equals(row2.get(0).getAsString());
            }
        });
        Relation studentsInMath = raImpl4.join(takesCourse, mathCourses, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return false;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return row1.get(1).getAsString().equals(row2.get(0).getAsString());
            }
        });
        Relation studentsInBoth = raImpl4.join(studentsInCompSci, studentsInMath, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return false;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return row1.get(0).getAsString().equals(row2.get(0).getAsString());
            }
        });

        Relation studentsWithA = raImpl4.select(takesCourse, gradeAPredicate);
        Relation studentsWithAIDs = raImpl4.project(studentsWithA, List.of("ID"));
        Relation finalStudents = raImpl4.diff(studentsInBoth, studentsWithAIDs);
        Relation studentDetails = raImpl4.join(finalStudents, students, new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return false;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return row1.get(0).getAsString().equals(row2.get(0).getAsString()); // Match student ID
            }
        });
        Relation finalResult4 = raImpl4.project(studentDetails, List.of("ID", "name", "dept_name", "tot_cred"));
        System.out.println("\nList of students enrolled in 'Comp. Sci.' and 'Math' but never received an 'A':");
        finalResult4.print();

        // interesting query #5 - Grace Ryoo
        // Years with instructors who taught in a building that's name begins with a vowel or in an even-numbered classroom
        Relation sectionRel = new RelationBuilder()
                .attributeNames(List.of("course_id", "sec_id", "semester", "year", "building", "room_number", "time_slot_id"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.INTEGER, Type.STRING, Type.STRING, Type.STRING))
                .build();
        sectionRel.loadData("4370project1/project_1_starter_code/src/main/java/uga/cs4370/data/mysql-files/section.csv");

        Relation teachesRel = new RelationBuilder()
                .attributeNames(List.of("ID", "course_id", "sec_id", "semester", "year"))
                .attributeTypes(List.of(Type.STRING, Type.STRING, Type.STRING, Type.STRING, Type.INTEGER))
                .build();
        teachesRel.loadData("4370project1/project_1_starter_code/src/main/java/uga/cs4370/data/mysql-files/teaches.csv");
        //  predidate checking for building name that begins with a vowel
        Predicate vowelPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                String buildingLetter = row.get(4).getAsString().toLowerCase();
                return buildingLetter.startsWith("a") || buildingLetter.startsWith("e") || buildingLetter.startsWith("i") || buildingLetter.startsWith("o") || buildingLetter.startsWith("u");
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        //  predidate checking for even-numbered classroom
        Predicate evenPredicate = new Predicate() {
            @Override
            public boolean check(List<Cell> row) {
                return Integer.parseInt(row.get(5).getAsString()) % 2 == 0;
            }

            @Override
            public boolean evaluate(List<Cell> row1, List<Cell> row2) {
                return false;
            }
        };

        RAImpl raImpl5 = new RAImpl();

        Relation filteredBuildings = raImpl5.select(sectionRel, vowelPredicate);
        Relation filteredRoomNums = raImpl5.select(sectionRel, evenPredicate);
        Relation buildingYears = raImpl5.project(filteredBuildings, List.of("year"));
        Relation roomYears = raImpl5.project(filteredRoomNums, List.of("year"));
        Relation vowelOrEvenYears = raImpl5.union(buildingYears, roomYears);
        Relation finalResult5 = raImpl5.rename(vowelOrEvenYears, List.of("year"), List.of("filtered_years"));
        System.out.println("\nYears with buildings that starts with a vowel or an even-numbered classroom:");
        finalResult5.print();

    }

}
