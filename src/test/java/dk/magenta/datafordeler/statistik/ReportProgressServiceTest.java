package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.statistik.reportExecution.ReportAssignment;
import dk.magenta.datafordeler.statistik.reportExecution.ReportSync;
import org.hibernate.Session;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.IOException;
import java.util.List;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class ReportProgressServiceTest extends TestBase {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    private TestUserDetails testUserDetails;


    @Test
    public void testQueueReport() throws IOException {


        try(Session sessionSync = sessionManager.getSessionFactory().openSession()) {
            ReportSync repSync = new ReportSync(sessionSync);
            ReportAssignment report = new ReportAssignment();
            report.setTemplateName("birthreport");
            repSync.startReport(report);
        }




        try(Session session = sessionManager.getSessionFactory().openSession()) {
            List<ReportAssignment> existingSubscriptions = QueryManager.getAllItems(session, ReportAssignment.class);
            Assert.assertEquals(1, existingSubscriptions.size());
        }



    }



}
