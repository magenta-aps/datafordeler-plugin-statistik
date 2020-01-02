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
import org.hibernate.jpa.QueryHints;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.core.task.SimpleAsyncTaskExecutor;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;

import java.io.IOException;
import java.io.OutputStream;

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
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
        super.handleRequest(request, response, ServiceName.COLLECTIVE);
    }


    @RequestMapping(method = RequestMethod.GET, path = "/reportexecuter/")
    public void geReportExecute(HttpServletRequest request, HttpServletResponse response) throws IOException {

        try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {

            CriteriaBuilder builder = reportProgressSession.getCriteriaBuilder();
            CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
            Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
            criteria.select(page);
            criteria.where(builder.and(
                    builder.equal(page.get(ReportAssignment.DB_FIELD_COLLECTIONUUID), request.getParameter("collectionUuid")),
                    builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.started)
            ));

            TypedQuery<ReportAssignment> query = reportProgressSession.createQuery(criteria);
            query.setHint(QueryHints.HINT_CACHEABLE, true);

            if(query.getResultList().size() > 0) {
                ReportAssignment assignment = query.getResultList().get(0);

                String formToken = URLEncoder.encode(request.getParameter("token"), StandardCharsets.UTF_8);

                String paramAppender = "";

                String registrationBefore = assignment.getRegistrationBefore();
                if(registrationBefore!=null) {
                    paramAppender+="registrationBefore="+registrationBefore+"&";
                }
                String registrationAfter = assignment.getRegistrationAfter();
                if(registrationAfter!=null) {
                    paramAppender+="registrationAfter="+registrationAfter+"&";
                }
                String collectionUuid = request.getParameter("collectionUuid");
                if(collectionUuid!=null) {
                    paramAppender+="collectionUuid="+collectionUuid+"&";
                }
                String reportUuid = assignment.getReportUuid();
                if(reportUuid!=null) {
                    paramAppender+="reportUuid="+reportUuid+"&";
                }

                paramAppender+="token="+formToken;

                response.sendRedirect("/statistik/"+assignment.getTemplateName()+"/?"+paramAppender);
            }
        }
    }


    @RequestMapping(method = RequestMethod.GET, path = "/done/")
    public void getDone(HttpServletRequest request, HttpServletResponse response) throws IOException {
        response.getOutputStream().write(("collectionUuid: "+request.getParameter("collectionUuid")).getBytes());
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
    public void handlePost(HttpServletRequest request, HttpServletResponse response) throws IOException {

        String currentcollectionUuid = "";

        try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {

            CriteriaBuilder builder = reportProgressSession.getCriteriaBuilder();
            CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
            Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
            criteria.select(page);
            criteria.where(builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.started));
            TypedQuery<ReportAssignment> query = reportProgressSession.createQuery(criteria);
            query.setHint(QueryHints.HINT_CACHEABLE, true);

            if(query.getResultList().size() > 0) {
                response.getOutputStream().write(("Reject starting any reports until current reports is done").getBytes());
                return;
            }

            reportProgressSession.beginTransaction();

            String registrationAfter = request.getParameter("registrationAfter");
            String registrationBefore = request.getParameter("registrationBefore");

            ReportAssignment birthReport = new ReportAssignment();
            birthReport.setTemplateName(ServiceName.BIRTH.getIdentifier());
            birthReport.setRegistrationBefore(registrationBefore);
            birthReport.setRegistrationAfter(registrationAfter);
            birthReport.setReportStatus(ReportProgressStatus.started);
            String collectionUuid = birthReport.getCollectionUuid();

            ReportAssignment deathReport = new ReportAssignment(collectionUuid);
            deathReport.setTemplateName(ServiceName.DEATH.getIdentifier());
            deathReport.setRegistrationBefore(registrationBefore);
            deathReport.setRegistrationAfter(registrationAfter);
            deathReport.setReportStatus(ReportProgressStatus.started);

            ReportAssignment movementReport = new ReportAssignment(collectionUuid);
            movementReport.setTemplateName(ServiceName.MOVEMENT.getIdentifier());
            movementReport.setRegistrationBefore(registrationBefore);
            movementReport.setRegistrationAfter(registrationAfter);
            movementReport.setReportStatus(ReportProgressStatus.started);

            ReportAssignment civilStatusReport = new ReportAssignment(collectionUuid);
            civilStatusReport.setTemplateName(ServiceName.CIVILSTATUS.getIdentifier());
            civilStatusReport.setRegistrationBefore(registrationBefore);
            civilStatusReport.setRegistrationAfter(registrationAfter);
            civilStatusReport.setReportStatus(ReportProgressStatus.started);

            reportProgressSession.save(birthReport);
            reportProgressSession.save(deathReport);
            reportProgressSession.save(movementReport);
            reportProgressSession.save(civilStatusReport);
            reportProgressSession.getTransaction().commit();
            currentcollectionUuid = collectionUuid;

        }

        String formToken = URLEncoder.encode(request.getParameter("token"), StandardCharsets.UTF_8);
        response.sendRedirect("/statistik/collective_report/reportexecuter/?"+"collectionUuid="+currentcollectionUuid+"&token="+formToken);

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
