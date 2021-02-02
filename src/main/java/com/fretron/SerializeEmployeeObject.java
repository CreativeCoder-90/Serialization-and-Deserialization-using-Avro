package com.fretron;

import com.fretron.model.Employee;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.specific.SpecificDatumWriter;

import java.io.File;
import java.io.IOException;

public class SerializeEmployeeObject {
    public static void main(String[] args) throws IOException {
        Employee e1 = new Employee();
        Employee e2 = new Employee();
        Employee e3 = new Employee();

        e1.setId("101");
        e1.setName("Tarun");
       // e1.setAge(24);

        e2.setId("102");
        e2.setName("Tulsi");
       // e2.setAge(25);

        e3.setId("103");
        e3.setName("Krishna");
       // e3.setAge(26);

        //lets convert java object to memory serialized format
        DatumWriter<Employee> employeeDatumWriter = new SpecificDatumWriter<Employee>(Employee.class);

        //This class writes a sequence serialized records of data conforming to a schema
        DataFileWriter<Employee> employeeFileWriter = new DataFileWriter<Employee>(employeeDatumWriter);

        employeeFileWriter.create(e2.getSchema(), new File("/home/shubham-fretron/IdeaProjects/Avro_Demo/src/main/avro/Employee.avro"));
        //provide the path of file where you want to store serialized version(byte stream) of Java object

        //To store the Employee data in Employee.avro file
        employeeFileWriter.append(e2);
        //employeeFileWriter.append(e1);
        //employeeFileWriter.append(e1);

        employeeFileWriter.close();
        System.out.println("Object of Employee has been serialized successfully");
    }
}
