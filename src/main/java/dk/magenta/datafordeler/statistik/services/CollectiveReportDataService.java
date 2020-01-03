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
import org.apache.commons.io.IOUtils;
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

import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.LocalDate;
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

    private static String reportTemplateLink = "<a>%s - %s</a><br>";


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

    @RequestMapping(method = RequestMethod.GET, path = "/reportlist/")
    public void getReportList(HttpServletRequest request, HttpServletResponse response) throws IOException {

        try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {

            String collectionUuid = request.getParameter("collectionUuid");

            CriteriaBuilder builder = reportProgressSession.getCriteriaBuilder();
            CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
            Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
            criteria.select(page);
            criteria.where(builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.started));

            TypedQuery<ReportAssignment> query = reportProgressSession.createQuery(criteria);
            query.setHint(QueryHints.HINT_CACHEABLE, true);

            String reportListResponse = "Started: <br>";

            for(ReportAssignment assignment : query.getResultList()) {
                String element = String.format(reportTemplateLink, assignment.getTemplateName(), assignment.getCollectionUuid());
                reportListResponse += element;
                collectionUuid = assignment.getCollectionUuid();
            }

            reportListResponse += "<br><br>Running: <br>";
            criteria.where(builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.running));
            query = reportProgressSession.createQuery(criteria);
            query.setHint(QueryHints.HINT_CACHEABLE, true);

            for(ReportAssignment assignment : query.getResultList()) {
                String element = String.format(reportTemplateLink, assignment.getTemplateName(), assignment.getCollectionUuid());
                reportListResponse += element;
            }
            //TODO: this could be done much better
            String listpage = IOUtils.resourceToString("/listServiceForm.html", StandardCharsets.UTF_8);

            String token = request.getParameter("token");
            String urlAttributes = collectionUuid;
            if(token!=null) {
                String formToken = URLEncoder.encode(token, StandardCharsets.UTF_8);
                urlAttributes += "&token="+formToken;
            }
            response.getOutputStream().write((String.format(listpage, reportListResponse, urlAttributes, urlAttributes)).getBytes());
        }
    }


    @RequestMapping(method = RequestMethod.GET, path = "/reportexecuter/")
    public void getReportExecute(HttpServletRequest request, HttpServletResponse response) throws IOException {

        try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {

            CriteriaBuilder builder = reportProgressSession.getCriteriaBuilder();
            CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
            Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
            criteria.select(page);
            String collectionUuid = request.getParameter("collectionUuid");
            if(collectionUuid!=null) {
                criteria.where(builder.and(
                        builder.equal(page.get(ReportAssignment.DB_FIELD_COLLECTIONUUID), collectionUuid),
                        builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.started)
                ));
            } else {
                criteria.where(builder.and(
                        builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.started)
                ));
            }

            TypedQuery<ReportAssignment> query = reportProgressSession.createQuery(criteria);
            query.setHint(QueryHints.HINT_CACHEABLE, true);

            if(query.getResultList().size() > 0) {
                ReportAssignment assignment = query.getResultList().get(0);

                ReportSyncHandler syncHandler = new ReportSyncHandler(reportProgressSession);
                if(syncHandler.hasReportsOfStatus(ReportProgressStatus.running)) {
                    response.getOutputStream().write(("There are allready running reports").getBytes());
                    return;
                }

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
                String assignmentCollectionUuid = assignment.getCollectionUuid();
                if(assignmentCollectionUuid!=null) {
                    paramAppender+="collectionUuid="+assignmentCollectionUuid+"&";
                }
                String reportUuid = assignment.getReportUuid();
                if(reportUuid!=null) {
                    paramAppender+="reportUuid="+reportUuid+"&";
                }

                paramAppender+="token="+formToken;

                response.sendRedirect("/statistik/"+assignment.getTemplateName()+"/?"+paramAppender);
            } else {
                response.getOutputStream().write(("No reports are waiting").getBytes());
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
                String formToken = URLEncoder.encode(request.getParameter("token"), StandardCharsets.UTF_8);
                response.sendRedirect("/statistik/collective_report/reportlist/?"+"collectionUuid="+currentcollectionUuid+"&token="+formToken);
                return;
            }

            reportProgressSession.beginTransaction();

            String registrationAfter = request.getParameter("registrationAfter");
            String registrationBefore = request.getParameter("registrationBefore");
            if(registrationAfter==null) {
                response.getOutputStream().write(("Reject the interval in reports").getBytes());
                return;
            }
            if(registrationBefore==null) {
                if(LocalDate.parse(registrationAfter).plusDays(120).isBefore(LocalDate.now())) {
                    response.getOutputStream().write(("Reject the interval in reports").getBytes());
                    return;
                }
            } else {
                if(LocalDate.parse(registrationAfter).plusDays(120).isBefore(LocalDate.parse(registrationBefore))) {
                    response.getOutputStream().write(("Reject the interval in reports").getBytes());
                    return;
                }
            }

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
        response.sendRedirect("/statistik/collective_report/reportlist/?"+"collectionUuid="+currentcollectionUuid+"&token="+formToken);

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
