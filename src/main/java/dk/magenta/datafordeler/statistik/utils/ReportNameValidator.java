package dk.magenta.datafordeler.statistik.utils;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class ReportNameValidator {

    private static String reportRegex = ".*?_([A-Z0-9]{8}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{4}-[A-Z0-9]{12})";

    public static boolean validateReportName(String reportName) {
        Pattern splitter = Pattern.compile(reportRegex, Pattern.CASE_INSENSITIVE | Pattern.DOTALL);
        Matcher matcher = splitter.matcher(reportName);
        return matcher.matches();
    }


}
