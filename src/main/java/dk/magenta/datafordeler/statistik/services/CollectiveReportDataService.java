package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.geo.GeoLookupService;
import dk.magenta.datafordeler.statistik.queries.PersonBirthQuery;
import dk.magenta.datafordeler.statistik.reportExecution.*;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.context.annotation.Bean;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.core.task.TaskExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

import java.util.*;
import java.util.concurrent.Callable;


@RestController
@RequestMapping("/statistik/collective_report")
public class CollectiveReportDataService extends PersonStatisticsService {



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




    @RequestMapping(value="/startreportsequence")
    public Callable<String> myControllerMethod(HttpServletRequest request) {
        Callable<String> asyncTask = () -> { try {

            DafoUserDetails user = this.getDafoUserManager().getUserFromRequest(request);
            String formToken = request.getParameter("token");
            if (formToken != null) {
                user = this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);

            }


            LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);
            loggerHelper.info("Incoming request for " + this.getClass().getSimpleName() + " with parameters " + request.getParameterMap());
            this.checkAndLogAccess(loggerHelper);

            String reportuUuid;

            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("TODOALL");
            try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {
                ReportSync repSync = new ReportSync(reportProgressSession);
                reportuUuid = repSync.setReportProgressObject(report);
                if (reportuUuid == null) {
                    return "Execution of this report is rejected, another report is currently getting generated";
                }
            }


            ReportExecutor executor = new ReportExecutor();

            ReportRunner simular = new ReportRunner(5, sessionManager);
            executor.execute(simular);

            System.out.println(" WAITING STARTED:"+new Date());
            Thread.sleep(1000);
            System.out.println(" WAITING COMPLETED:"+new Date());

            return reportuUuid;//Send the result back to View Return.jsp
        } catch(InterruptedException iexe) {
            //log exception
            return "ReturnFail";
        }};
        return asyncTask;
    }





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

        String showfrontpage= request.getParameter("showfrontpage");

        IOUtils.copy(
                StatisticsService.class.getResourceAsStream("/collectiveServiceForm.html"),
                response.getWriter(), StandardCharsets.UTF_8
        );


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
        super.handleRequest(request, response, ServiceName.ALL);
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
