package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.AccessDeniedException;
import dk.magenta.datafordeler.core.exception.AccessRequiredException;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.reportExecution.ReportAssignment;
import dk.magenta.datafordeler.statistik.reportExecution.ReportSync;
import dk.magenta.datafordeler.statistik.services.BirthDataService;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.ReportValidationAndConversion;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
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

import java.io.IOException;
import java.io.OutputStream;
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
    public void testQueueReport() throws IOException {


        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSync repSync = new ReportSync(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("REPORT1");
            Assert.assertNotNull(repSync.setReportProgressObject(report));
        }

        try(Session session = sessionManager.getSessionFactory().openSession()) {
            List<ReportAssignment> existingSubscriptions = QueryManager.getAllItems(session, ReportAssignment.class);
            Assert.assertEquals(1, existingSubscriptions.size());
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSync repSync = new ReportSync(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("REPORT2");
            Assert.assertNotNull(repSync.setReportProgressObject(report));
        }

        try(Session session = sessionManager.getSessionFactory().openSession()) {
            List<ReportAssignment> existingSubscriptions = QueryManager.getAllItems(session, ReportAssignment.class);
            Assert.assertEquals(2, existingSubscriptions.size());
        }

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSync repSync = new ReportSync(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("REPORT1");
            Assert.assertNull(repSync.setReportProgressObject(report));
        }
    }


    @Test
    public void testReportnameValidation() throws IOException {
        Assert.assertTrue(ReportValidationAndConversion.validateReportName("REPORTNAME_f7dcb5b3-4590-4e24-9cb1-b01f5bf1821b"));
        Assert.assertTrue(ReportValidationAndConversion.validateReportName("T_f7dcb5b3-4590-4e24-9cb1-b01f5bf1821c"));
        Assert.assertFalse(ReportValidationAndConversion.validateReportName("REPORTNAMEf7dcb5b3-4590-4e24-9cb1-b01f5bf1821b"));
        Assert.assertFalse(ReportValidationAndConversion.validateReportName("REPORTNAME_f7dcb5b32-4590-4e24-9cb1-b01f5bf1821"));
    }



    @Test
    public void testRejectSimultaniousGet() throws JsonProcessingException {
        birthDataService.setWriteToLocalFile(false);
        birthDataService.setUseTimeintervallimit(false);

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSync repSync = new ReportSync(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("BIRTH");
            Assert.assertNotNull(repSync.setReportProgressObject(report));
        }

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?registrationAfter=2000-01-01&afterDate=1999-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(409, response.getStatusCodeValue());
    }

    @Test
    public void testRejectSimultaniousPost() throws IOException {
        birthDataService.setWriteToLocalFile(true);
        birthDataService.setUseTimeintervallimit(false);

        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSync repSync = new ReportSync(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("BIRTH");
            Assert.assertNotNull(repSync.setReportProgressObject(report));
        }

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?registrationAfter=2000-01-01", HttpMethod.POST, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(409, response.getStatusCodeValue());
    }
}
