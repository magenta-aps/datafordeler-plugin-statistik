package dk.magenta.datafordeler.statistik.utils;

import net.lingala.zip4j.core.ZipFile;
import net.lingala.zip4j.exception.ZipException;
import net.lingala.zip4j.model.ZipParameters;
import net.lingala.zip4j.util.Zip4jConstants;

import java.io.File;
import java.util.ArrayList;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReportValidationAndConversion {

    private static String reportRegex = ".*?_([A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12})";

    public static boolean validateReportName(String reportName) {
        Pattern splitter = Pattern.compile(reportRegex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = splitter.matcher(reportName);
        return matcher.matches();
    }


    public static synchronized void convertFileToEncryptedZip(File reportName, ArrayList<File> filesToAdd, String password) throws ZipException {
        //This is name and path of zip file to be created
        ZipFile zipFile = new ZipFile(reportName);

        ZipParameters parameters = new ZipParameters();
        parameters.setCompressionMethod(Zip4jConstants.COMP_DEFLATE); // set compression method to deflate compression
        parameters.setCompressionLevel(Zip4jConstants.DEFLATE_LEVEL_NORMAL);

        parameters.setEncryptFiles(true);

        //Set the encryption method to AES Zip Encryption
        parameters.setEncryptionMethod(Zip4jConstants.ENC_METHOD_STANDARD);
        parameters.setAesKeyStrength(Zip4jConstants.AES_STRENGTH_256);

        //Set password
        parameters.setPassword(password);

        //Now add files to the zip file
        zipFile.addFiles(filesToAdd, parameters);
    }


}
