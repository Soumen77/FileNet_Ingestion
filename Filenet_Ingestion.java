package com.ingestion;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileReader;
import java.util.ArrayList;
import java.util.InputMismatchException;
import java.util.Iterator;
import java.util.List;
import java.util.Scanner;

import javax.security.auth.Subject;

import com.filenet.api.collection.ContentElementList;
import com.filenet.api.collection.ObjectStoreSet;
import com.filenet.api.constants.AutoClassify;
import com.filenet.api.constants.AutoUniqueName;
import com.filenet.api.constants.CheckinType;
import com.filenet.api.constants.DefineSecurityParentage;
import com.filenet.api.constants.RefreshMode;
import com.filenet.api.core.Connection;
import com.filenet.api.core.ContentTransfer;
import com.filenet.api.core.Document;
import com.filenet.api.core.Domain;
import com.filenet.api.core.Factory;
import com.filenet.api.core.Folder;
import com.filenet.api.core.ObjectStore;
import com.filenet.api.core.ReferentialContainmentRelationship;
import com.filenet.api.util.UserContext;

public class Filenet_Ingestion {
	private static ObjectStore objectStore = null;
    private static Connection connection = null;
    private static final String uri = "http://localhost:9080/wsi/FNCEWS40MTOM/";
    private static final String username = "wasadmin";
    private static final String password = "Password123";
    private static final String stanza = "FileNetP8Server";
    private static int successfullyUploaded = 0;
    private static int failedUploads = 0;

    public static void main(String[] args) {
        initializeConnection();

        // Fetch the list of object stores
        List<ObjectStore> objectStores = getObjectStores(connection);

        // Prompt the user to select an object store
        objectStore = promptUserForSelection(objectStores);

        // Prompt the user for the Excel file path
        Scanner scanner = new Scanner(System.in);
//        System.out.print("Enter the Excel file path: ");
//        String excelFilePath = scanner.nextLine();
        System.out.print("Enter the CSV file path: ");
        String csvFilePath = scanner.nextLine();

        // Prompt the user to enter the document class name
        System.out.print("Enter the document class name: ");
        String documentClass = scanner.nextLine();

        // Prompt the user to enter the folder name
        System.out.print("Enter the folder name (e.g., YourFileNetFolder): ");
        String folderName = scanner.nextLine();

        // Prepend and append slashes to create the folder path
        String folderPath = "/" + folderName + "/";

        
     
        

        // Read data from Excel and upload documents
        readCsvAndUpload(csvFilePath, folderPath, documentClass);

        scanner.close();

        System.out.println("Summary:");
        System.out.println("Successfully Uploaded: " + successfullyUploaded);
        System.out.println("Failed Uploads: " + failedUploads);
    }

   
    

    private static void initializeConnection() {
        try {
            connection = Factory.Connection.getConnection(uri);
            Subject sub = UserContext.createSubject(connection, username, password, stanza);
            UserContext.get().pushSubject(sub);
            System.out.println("\n\n Connection to FileNet successful !!");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private static List<ObjectStore> getObjectStores(Connection connection) {
        List<ObjectStore> objectStores = new ArrayList<>();
        try {
            Domain domain = Factory.Domain.fetchInstance(connection, null, null);
            ObjectStoreSet objectStoreSet = domain.get_ObjectStores();
            Iterator iterator = objectStoreSet.iterator();

            while (iterator.hasNext()) {
                ObjectStore objectStore = (ObjectStore) iterator.next();
                objectStores.add(objectStore);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
        return objectStores;
    }

    private static ObjectStore promptUserForSelection(List<ObjectStore> objectStores) {
        System.out.println("Available Object Stores:");
        for (int i = 0; i < objectStores.size(); i++) {
            System.out.println((i + 1) + ". " + objectStores.get(i).get_DisplayName());
        }

        int selectedOption;
        do {
            System.out.println("Select an Object Store (1-" + objectStores.size() + "):");
            Scanner scanner = new Scanner(System.in);
            try {
                selectedOption = scanner.nextInt();
            } catch (InputMismatchException e) {
                selectedOption = 0;
            }
        } while (selectedOption < 1 || selectedOption > objectStores.size());

        return objectStores.get(selectedOption - 1);
    }
    private static void readCsvAndUpload(String csvFilePath, String folderPath, String documentClass) {
        try (BufferedReader br = new BufferedReader(new FileReader(csvFilePath))) {
            String line;
            br.readLine(); // Skip header line

            while ((line = br.readLine()) != null) {
                String[] values = line.split(",");

                String fullName = values[0];
                String branch = values[1];
                int age = Integer.parseInt(values[2]);
                String address = values[3];
                String gender = values[4];
                String filePath = values[5];

                uploadDocument(folderPath, documentClass, fullName, branch, age, address, gender, filePath);
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
//For Excel sheeet Retrive of file but jar file is not working..
    /*private static void readcsvAndUpload(String excelFilePath, String folderPath, String documentClass) {
        try (InputStream inp = new FileInputStream(excelFilePath)) {
            Workbook workbook = WorkbookFactory.create(inp);
            Sheet sheet = workbook.getSheetAt(0); // Assuming data is in the first sheet

            Iterator<Row> rowIterator = sheet.iterator();
            rowIterator.next(); // Skip header row

            while (rowIterator.hasNext()) {
                Row row = rowIterator.next();
                String fullName = row.getCell(0).getStringCellValue(); // Assuming first column has full name
                String branch = row.getCell(1).getStringCellValue(); // Assuming second column has branch
                int age = (int) row.getCell(2).getNumericCellValue(); // Assuming third column has age
                String address = row.getCell(3).getStringCellValue(); // Assuming fourth column has address
                String gender = row.getCell(4).getStringCellValue(); // Assuming fifth column has gender
                String filePath = row.getCell(5).getStringCellValue(); // Assuming sixth column has file path

                uploadDocument(folderPath, documentClass, fullName, branch, age, address, gender, filePath);
            }

            workbook.close();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }*/

    private static void uploadDocument(String folderPath, String documentClass, String fullName, String branch,
                                       int age, String address, String gender, String filePath) {
        try (FileInputStream fis = new FileInputStream(new File(filePath))) {
            Folder folder = Factory.Folder.fetchInstance(objectStore, folderPath, null);

            Document doc = Factory.Document.createInstance(objectStore, documentClass);

            ContentElementList contEleList = Factory.ContentElement.createList();
            ContentTransfer ct = Factory.ContentTransfer.createInstance();

            ct.setCaptureSource(fis);
            ct.set_ContentType("pdf/plain");
            ct.set_RetrievalName(new File(filePath).getName());
            contEleList.add(ct);

            doc.set_ContentElements(contEleList);
            doc.getProperties().putValue("DocumentTitle", fullName);

            doc.getProperties().putValue("FullName", fullName);
            doc.getProperties().putValue("Branch", branch);
            doc.getProperties().putValue("Age", age);
            doc.getProperties().putValue("Address", address);
            doc.getProperties().putValue("Gender", gender);
            
            String filepathValue = filePath; // Make sure 'filePath' is the correct value
            doc.getProperties().putValue("Filepath", filepathValue);
            
            doc.set_MimeType("pdf/plain");
            doc.checkin(AutoClassify.AUTO_CLASSIFY, CheckinType.MAJOR_VERSION);
            doc.save(RefreshMode.REFRESH);

            ReferentialContainmentRelationship rcr = folder.file(doc, AutoUniqueName.AUTO_UNIQUE,
                    new File(filePath).getName(), DefineSecurityParentage.DO_NOT_DEFINE_SECURITY_PARENTAGE);
            rcr.save(RefreshMode.REFRESH);

            String documentId = doc.get_Id().toString();

            System.out.println("Document added successfully to folder: " + folderPath);
            System.out.println("Document ID: " + documentId); // Print Document ID
            successfullyUploaded++;
        } catch (Exception e) {
            e.printStackTrace();
            System.err.println("Error adding document: " + e.getMessage());
            failedUploads++;
        }
    }
}
