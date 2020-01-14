package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.reportExecution.ReportAssignment;
import dk.magenta.datafordeler.statistik.reportExecution.ReportProgressStatus;
import dk.magenta.datafordeler.statistik.reportExecution.ReportSyncHandler;
import dk.magenta.datafordeler.statistik.services.BirthDataService;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.hibernate.Session;
import org.hibernate.jpa.QueryHints;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.annotation.DirtiesContext;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class ReportProgressServiceTest extends TestBase {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    private TestUserDetails testUserDetails;

    @Autowired
    private BirthDataService birthDataService;//Just one of the reportservices to use in test


    @Test
    public void testQueueReport() throws Exception {

        String reportUuid = null;
        String reportCollectionUuid = null;

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            reportUuid = report.getReportUuid();
            reportCollectionUuid = report.getCollectionUuid();
            report.setTemplateName("REPORT1");
            Assert.assertTrue(repSync.createReportStatusObject(report));
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {

            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);

            CriteriaBuilder builder = sessionSync.getCriteriaBuilder();
            CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
            Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
            criteria.select(page);
            criteria.where(builder.and(
                    builder.equal(page.get(ReportAssignment.DB_FIELD_REPORTUUID), reportCollectionUuid),
                    builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.started)
            ));

            TypedQuery<ReportAssignment> query = sessionSync.createQuery(criteria);
            query.setHint(QueryHints.HINT_CACHEABLE, true);

            Assert.assertEquals(1, repSync.getReportList(reportCollectionUuid, ReportProgressStatus.started).size());

        }



        try(Session session = sessionManager.getSessionFactory().openSession()) {
            List<ReportAssignment> existingSubscriptions = QueryManager.getAllItems(session, ReportAssignment.class);
            Assert.assertEquals(1, existingSubscriptions.size());
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("REPORT2");
            Assert.assertFalse(repSync.createReportStatusObject(report));
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            repSync.setReportStatus(reportUuid, ReportProgressStatus.done);
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("REPORT2");
            Assert.assertTrue(repSync.createReportStatusObject(report));
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("REPORT1");
            Assert.assertFalse(repSync.createReportStatusObject(report));
        }
    }


    @Test
    public void testRejectSimultaniousGet() throws Exception {
        birthDataService.setWriteToLocalFile(false);
        birthDataService.setUseTimeintervallimit(false);

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName(StatisticsService.ServiceName.BIRTH.getIdentifier());
            Assert.assertTrue(repSync.createReportStatusObject(report));
        }

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?registrationAfter=2000-01-01&afterDate=1999-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(409, response.getStatusCodeValue());
    }

    @Test
    public void testRejectSimultaniousPost() throws Exception {
        birthDataService.setWriteToLocalFile(true);
        birthDataService.setUseTimeintervallimit(false);

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName(StatisticsService.ServiceName.BIRTH.getIdentifier());
            Assert.assertTrue(repSync.createReportStatusObject(report));
        }

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?registrationAfter=2000-01-01", HttpMethod.POST, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(409, response.getStatusCodeValue());

    }

    @Test
    public void testReportProgressService() throws Exception {
        birthDataService.setWriteToLocalFile(true);
        birthDataService.setUseTimeintervallimit(false);

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSyncHandler repSync = new ReportSyncHandler(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName(StatisticsService.ServiceName.BIRTH.getIdentifier());
            Assert.assertTrue(repSync.createReportStatusObject(report));

            ResponseEntity<String> response = restTemplate.exchange("/statistik/collective_report/reportstatus/?collectionUuid="+report.getCollectionUuid(), HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
            Assert.assertEquals("started,\n", response.getBody());
        }
    }


}
