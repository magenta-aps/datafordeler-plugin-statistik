package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.geo.GeoLookupService;
import dk.magenta.datafordeler.statistik.reportExecution.*;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;

import java.util.*;



@RestController
@RequestMapping("/statistik/collective_report")
public class CollectiveReportDataService extends PersonStatisticsService {

    private class Exclude extends Exception {
    }

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    @Autowired
    private CprPlugin cprPlugin;

    private Logger log = LogManager.getLogger(CollectiveReportDataService.class);

    /**
     * Calls handleRequest in super with the ID of the report as a parameter
     * @param request
     * @param response
     * @throws AccessDeniedException
     * @throws AccessRequiredException
     * @throws InvalidTokenException
     * @throws IOException
     * @throws MissingParameterException
     * @throws InvalidClientInputException
     * @throws HttpNotFoundException
     * @throws InvalidCertificateException
     */
    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.BIRTH);
    }

    /**
     * Post is used for starting the generation of a report
     * @param request
     * @param response
     * @throws AccessDeniedException
     * @throws AccessRequiredException
     * @throws InvalidTokenException
     * @throws IOException
     * @throws MissingParameterException
     * @throws InvalidClientInputException
     * @throws HttpNotFoundException
     * @throws InvalidCertificateException
     */
    @RequestMapping(method = RequestMethod.POST, path = "/")
    public void handlePost(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {




        try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {

            String outputDescription = null;
            OutputStream outputStream = null;
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("ALL1");
            ReportSync repSync = new ReportSync(reportProgressSession);
            String reportuUuid = repSync.setReportProgressObject(report);
            repSync.setReportStatus(ReportProgressStatus.started);


            report = new ReportAssignment();
            report.setTemplateName("ALL2");
            repSync = new ReportSync(reportProgressSession);
            reportuUuid = repSync.setReportProgressObject(report);
            repSync.setReportStatus(ReportProgressStatus.started);

            report = new ReportAssignment();
            report.setTemplateName("ALL3");
            repSync = new ReportSync(reportProgressSession);
            reportuUuid = repSync.setReportProgressObject(report);
            repSync.setReportStatus(ReportProgressStatus.started);

        }





        /*super.handleRequest(request, response, ServiceName.BIRTH);




        String formToken = URLEncoder.encode(request.getParameter("token"), StandardCharsets.UTF_8);

        String registrationAfter = request.getParameter("registrationAfter");
        String registrationBefore = request.getParameter("registrationBefore");
        response.sendRedirect("/statistik/death_data/?"+"registrationAfter="+registrationAfter+"&registrationBefore="+registrationBefore+"&token="+formToken);
*/

    }

    private static final String OWN_PREFIX = "B_";

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{});
    }

    @Override
    protected SessionManager getSessionManager() {
        return this.sessionManager;
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }


    @Override
    protected DafoUserManager getDafoUserManager() {
        return this.dafoUserManager;
    }

    @Override
    protected Logger getLogger() {
        return this.log;
    }

    protected String[] requiredParameters() {
        return new String[]{"registrationAfter"};
    }

    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {
        return null;
    }

    @Override
    protected PersonRecordQuery getQuery(Filter filter) {
        return null;
    }

    public class ReportExecutor extends SimpleAsyncTaskExecutor {

    }

}
