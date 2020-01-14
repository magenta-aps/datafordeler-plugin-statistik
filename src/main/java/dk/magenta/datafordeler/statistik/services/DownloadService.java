package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.AccessDeniedException;
import dk.magenta.datafordeler.core.exception.AccessRequiredException;
import dk.magenta.datafordeler.core.exception.InvalidTokenException;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.StatistikRolesDefinition;
import dk.magenta.datafordeler.statistik.reportExecution.ReportProgressStatus;
import dk.magenta.datafordeler.statistik.reportExecution.ReportSyncHandler;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.ReportValidationAndConversion;
import net.lingala.zip4j.exception.ZipException;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;


@RestController
@RequestMapping("/statistik/download_data")
public class DownloadService extends StatisticsService {

    @Autowired
    private DafoUserManager dafoUserManager;

    private Logger log = LogManager.getLogger(DownloadService.class);

    @RequestMapping(method = RequestMethod.GET, path = "/")
    protected void doGet(HttpServletRequest request,
                          HttpServletResponse response) throws IOException {

        String formToken = request.getParameter("token");
        String collectionUuid = request.getParameter("collectionUuid");

        if(formToken!=null && collectionUuid!=null) {
            IOUtils.copy(
                    StatisticsService.class.getResourceAsStream("/downloadSimpleServiceForm.html"),
                    response.getWriter(), StandardCharsets.UTF_8
            );
        } else {
            IOUtils.copy(
                    StatisticsService.class.getResourceAsStream("/downloadServiceForm.html"),
                    response.getWriter(), StandardCharsets.UTF_8
            );
        }
        return;
    }


    @RequestMapping(method = RequestMethod.POST, path = "/")
    protected void doPost(HttpServletRequest request,
                         HttpServletResponse response) throws IOException, AccessDeniedException, AccessRequiredException, InvalidTokenException {

        String password = request.getParameter("password");

        String formToken = request.getParameter("token");
        DafoUserDetails user = null;
        if (formToken != null) {
            user = this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);
        }

        // Add current user to LoggerHelper
        LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);

        log.info(this.getClass().getSimpleName());
        log.info(request.getParameterMap());

        loggerHelper.info("Incoming request for " + this.getClass().getSimpleName() + " with parameters " + request.getParameterMap());

        // Check that the user has access to CPR data
        this.checkAndLogAccess(loggerHelper);

        String reportId = request.getParameter("collectionUuid");

        // obtains response's output stream
        OutputStream outStream = response.getOutputStream();

        if(password.length()<8) {
            outStream.write("Password must be at least 8 characters".getBytes(StandardCharsets.UTF_8));
            outStream.close();
            return;
        }

        //Add files to be archived into zip file
        ArrayList<File> filesToAdd = new ArrayList<File>();

        try {
            try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {

                ReportSyncHandler repSync = new ReportSyncHandler(reportProgressSession);
                List<String> reportList = repSync.getReportList(reportId, ReportProgressStatus.done);
                if (reportList.size()==0) {
                    outStream.write("Report does not exist".getBytes(StandardCharsets.UTF_8));
                    outStream.close();
                    return;
                }
                for(String report : reportList) {
                    if (!ReportValidationAndConversion.validateReportName(report)) {
                        outStream.write("Illegal reportname".getBytes(StandardCharsets.UTF_8));
                        outStream.close();
                        return;
                    }
                    filesToAdd.add(new File(PATH_FILE,report + ".csv"));
                }
                ReportValidationAndConversion.convertFileToEncryptedZip(new File(PATH_FILE,reportId+".zip"), filesToAdd, password);
            }

        } catch (ZipException e) {
            log.error("Unable to encrypt reportfile", e);
        }

        // reads input file from an absolute path
        String filePath = reportId+".zip";
        File downloadFile = new File(StatisticsService.PATH_FILE, filePath);
        if (!downloadFile.exists()) {
            outStream.write("Report does not exist".getBytes(StandardCharsets.UTF_8));
            outStream.close();
            return;
        }
        FileInputStream inStream = new FileInputStream(downloadFile);

        response.setContentLength((int) downloadFile.length());

        // forces download
        String headerKey = "Content-Disposition";
        String headerValue = String.format("attachment; filename=\"%s\"", downloadFile.getName());
        response.setHeader(headerKey, headerValue);

        byte[] buffer = new byte[4096];
        int bytesRead;

        while ((bytesRead = inStream.read(buffer)) != -1) {
            outStream.write(buffer, 0, bytesRead);
        }

        inStream.close();
        outStream.close();
    }

    @Override
    public int run(Filter filter, OutputStream outputStream, String reportUuid) {
        return 0;
    }

    @Override
    protected List<String> getColumnNames() {
        return null;
    }

    @Override
    protected SessionManager getSessionManager() {
        return null;
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return null;
    }

    @Override
    protected DafoUserManager getDafoUserManager() {
        return this.dafoUserManager;
    }

    @Override
    protected Logger getLogger() {
        return this.log;
    }
}
