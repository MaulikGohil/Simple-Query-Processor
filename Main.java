package com.queryprocessor;

import java.io.BufferedReader;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

public class Main {

	public static void main(String[] args) throws IOException {
	
		BufferedReader exeStepsReader = new BufferedReader(new FileReader("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\inputFile.txt"));
		
		String step;
		while ((step=exeStepsReader.readLine()) != null) 
		 {		
			 String[] detailedStep = step.split(" ");
			 String comp = detailedStep[0];
			 
			 if(comp.equals("Selection"))
			 {
				 String selectTblName = detailedStep[1];
				 String whereCond = detailedStep[2];
				 String targetTblName = detailedStep[3];
				 BufferedReader csvReaderEmployee = new BufferedReader(new FileReader("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+selectTblName+".csv"));
				 FileWriter csvWriter = new FileWriter("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+targetTblName+".csv");
				 
				 String[] condition = whereCond.split("=");
				 String attribute = condition[0];
				 int attributeValue = Integer.parseInt(condition[1]);
				 
				 String Line = csvReaderEmployee.readLine();
				 String[] LineSplit = Line.split(",");
				 
				 for(String Data : LineSplit)
				    {
				    	csvWriter.append(String.join(",", Data));
					    csvWriter.append(",");
				    } 
				 csvWriter.append("\n");
				 
				 String row;
				 while ((row = csvReaderEmployee.readLine()) != null)
				 {
					 
				 	String[] Columns = row.split(",");
				 	int v = Integer.parseInt(Columns[9]);
				 					 	
				 	if(attributeValue == v)
				 	{
				 		for(String Data : Columns)
					    {
					    	csvWriter.append(String.join(",", Data));
						    csvWriter.append(",");
					    } 
				 		csvWriter.append("\n");
				 	}
				 	
				 }
			 csvWriter.close();	
			 csvReaderEmployee.close();
			 }
			 
			 if(comp.equals("Join"))
			 {
				 String selectTblName = detailedStep[1];
				 String selectTblName1 = detailedStep[2];
				 String joinCond = detailedStep[3];
				 String targetTblName = detailedStep[4];
				 
				 BufferedReader csvReaderEmpDno5 = new BufferedReader(new FileReader("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+selectTblName+".csv"));
				 BufferedReader csvReaderDepartment = new BufferedReader(new FileReader("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+selectTblName1+".csv"));
				 FileWriter csvWriter = new FileWriter("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+targetTblName+".csv");
				 
				 String[] tempCond = joinCond.split("=");
				 String joinCol1 = tempCond[0];
				 String joinCol2 = tempCond[1];
				 
				 String LineTbl1 = csvReaderEmpDno5.readLine();
				 String[] LineSplit1 = LineTbl1.split(",");
				 for(String Data : LineSplit1)
				    {
				    	csvWriter.append(String.join(",", Data));
					    csvWriter.append(",");
				    }
				 String LineTbl2 = csvReaderDepartment.readLine();
				 String[] LineSplit2 = LineTbl2.split(",");
				 for(String Data : LineSplit2)
				    {
				    	csvWriter.append(String.join(",", Data));
					    csvWriter.append(",");
				    } 
				 csvWriter.append("\n");
				 
				 String row;
				 String rowDept;
				 int v1,v2;
				 while ((row = csvReaderEmpDno5.readLine()) != null)
				 {
				 	String[] ColumnsDno5 = row.split(",");
				 	v1 = Integer.parseInt(ColumnsDno5[9]);
				 	BufferedReader csvReaderDepartmentNew = new BufferedReader(new FileReader("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+selectTblName1+".csv"));
				 	
				 	rowDept = csvReaderDepartmentNew.readLine();
				 	while ((rowDept = csvReaderDepartmentNew.readLine()) != null)
				 	{
					 	String[] ColumnsDept = rowDept.split(",");
					 	v2 = Integer.parseInt(ColumnsDept[1]);
					 					 	
					 	if(v1 == v2)
					 	{
					 		for(String Data : ColumnsDno5)
						    {
						    	csvWriter.append(String.join(",", Data));
							    csvWriter.append(",");
						    }
					 		for(String Data : ColumnsDept)
						    {
						    	csvWriter.append(String.join(",", Data));
							    csvWriter.append(",");
						    }
					 		csvWriter.append("\n");
					 	}
				 	}
				 	csvReaderDepartmentNew.close();
				 }
			 csvWriter.close();
			 csvReaderEmpDno5.close();
			 csvReaderDepartment.close();
			 }
			 
			 if(comp.equals("Projection"))
			 {
				 String selectTblName = detailedStep[1];
				 String projectionCond = detailedStep[2];
				 String targetTblName = detailedStep[3];
				 String[] attribute = projectionCond.split(",");
				 
				 BufferedReader csvReaderEmpDept = new BufferedReader(new FileReader("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+selectTblName+".csv"));
				 FileWriter csvWriter = new FileWriter("D:\\SEM1\\CIS611_Enterprise DB and Warehouse\\Labs\\Lab2\\"+targetTblName+".csv");
				 
				 for(String Data : attribute)
				    {
				    	csvWriter.append(String.join(",", Data));
					    csvWriter.append(",");
				    } 
				 csvWriter.append("\n");
				 
				 String output = csvReaderEmpDept.readLine();
				 while ((output = csvReaderEmpDept.readLine()) != null)
				 {
					 System.out.println("Ys023910");
					 String[] ColumnsEmpDept = output.split(",");
					 String v1 = ColumnsEmpDept[0];
					 String v2 = ColumnsEmpDept[2];
					 String v3 = ColumnsEmpDept[3];
					 String v4 = ColumnsEmpDept[9];
					 String v5 = ColumnsEmpDept[10];
					 
					 csvWriter.append(String.join(",", v1));
					 csvWriter.append(",");
					 csvWriter.append(String.join(",", v2));
					 csvWriter.append(",");
					 csvWriter.append(String.join(",", v3));
					 csvWriter.append(",");
					 csvWriter.append(String.join(",", v4));
					 csvWriter.append(",");
					 csvWriter.append(String.join(",", v5));
					 csvWriter.append(",");
					 csvWriter.append("\n");
				 }
			 csvWriter.close();
			 csvReaderEmpDept.close();
			 }
			 
		 }
	exeStepsReader.close();
	}
}
