package com.fretron;

import com.fretron.model.Employee;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.io.DatumReader;
import org.apache.avro.specific.SpecificDatumReader;
import java.io.File;
import java.io.IOException;

public class DeserializeEmployeeObject {
    public static void main(String[] args) throws IOException {

        //To deserialize the employee object
        DatumReader<Employee> employeeDatumReader= new SpecificDatumReader<>(Employee.class);

        //here this class reads the serialized data from file Employee.avro
        DataFileReader<Employee> employeeDataFileReader= new DataFileReader<>(
                new File("/home/shubham-fretron/IdeaProjects/Avro_Demo/src/main/avro/Employee.avro"), employeeDatumReader);

        Employee emp=null;
        while(employeeDataFileReader.hasNext()){
            emp = employeeDataFileReader.next(emp);
            System.out.println(emp.toString());
        }
    }
}
