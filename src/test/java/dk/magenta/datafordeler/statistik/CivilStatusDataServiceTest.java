package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.statistik.services.CivilStatusDataService;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.hibernate.Session;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;

import static org.junit.Assert.assertNotNull;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class CivilStatusDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    @Autowired
    private CivilStatusDataService civilStatusDataService;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private TestUtil testUtil;

    @Autowired
    private ObjectMapper objectMapper;

    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadPersonData("marriedperson.txt");
        testsUtils.loadGladdrregData();
    }

    @After
    public void cleanup() {
        testsUtils.clearPath();
        testsUtils.deleteAll();
    }

    @Test
    public void testMarriageInitiated() throws JsonProcessingException {

        Session session = sessionManager.getSessionFactory().openSession();
        List<PersonEntity> personEntities = QueryManager.getAllEntities(session, PersonEntity.class);
        PersonEntity personEntity = personEntities.get(0);
        Assert.assertEquals(4, personEntity.getCivilstatus().size());
    }


    @Test
    public void testServiceMarried() throws JsonProcessingException {
        civilStatusDataService.setWriteToLocalFile(false);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/civilstate_data/?CivSt=G&registrationAfter=1980-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/civilstate_data/?CivSt=G&registrationAfter=1980-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response body", response.getBody());
        String expected = "\"CivSt\";\"CivDto\";\"StatKod\";\"ProdDto\";\"Pnr\";\"AegtePnr\";\"authority\";\"KomKod\";\"FoedMynKod\";\"FoedMynTxt\";\"FoedMynKodTxt\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"G\";\"05-10-1990\";;\"22-05-1989\";\"0123456789\";\"0202501111\";\"1316\";;\"9507\";\"\";\"0\";;;;;;;;\n" +
                "\"G\";\"01-03-1998\";\"5100\";\"23-09-1991\";\"0123456789\";\"0303501111\";\"1316\";\"955\";\"9507\";\"\";\"0\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );

        response = restTemplate.exchange("/statistik/civilstate_data/?registrationAfter=2000-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response body", response.getBody());
        expected = "\"CivSt\";\"CivDto\";\"StatKod\";\"ProdDto\";\"Pnr\";\"AegtePnr\";\"authority\";\"KomKod\";\"FoedMynKod\";\"FoedMynTxt\";\"FoedMynKodTxt\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"E\";\"02-08-2018\";\"5100\";\"23-09-1991\";\"0123456789\";\"0303501111\";\"1316\";\"955\";\"9507\";\"\";\"0\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );

        response = restTemplate.exchange("/statistik/civilstate_data/?registrationAfter=1980-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response body", response.getBody());
        expected = "\"CivSt\";\"CivDto\";\"StatKod\";\"ProdDto\";\"Pnr\";\"AegtePnr\";\"authority\";\"KomKod\";\"FoedMynKod\";\"FoedMynTxt\";\"FoedMynKodTxt\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"G\";\"05-10-1990\";;\"22-05-1989\";\"0123456789\";\"0202501111\";\"1316\";;\"9507\";\"\";\"0\";;;;;;;;\n" +
                "\"G\";\"01-03-1998\";\"5100\";\"23-09-1991\";\"0123456789\";\"0303501111\";\"1316\";\"955\";\"9507\";\"\";\"0\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"\n" +
                "\"F\";\"12-05-1995\";\"5100\";\"11-09-1991\";\"0123456789\";\"0202501111\";\"1316\";;\"9507\";\"\";\"0\";;;;;;;;\n" +
                "\"E\";\"02-08-2018\";\"5100\";\"23-09-1991\";\"0123456789\";\"0303501111\";\"1316\";\"955\";\"9507\";\"\";\"0\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );
    }


    @Test
    public void testServiceWidow() throws JsonProcessingException {

        civilStatusDataService.setWriteToLocalFile(false);
        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/civilstate_data/?CivSt=E&registrationAfter=1980-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response body", response.getBody());
        String expected = "\"CivSt\";\"CivDto\";\"StatKod\";\"ProdDto\";\"Pnr\";\"AegtePnr\";\"authority\";\"KomKod\";\"FoedMynKod\";\"FoedMynTxt\";\"FoedMynKodTxt\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"E\";\"02-08-2018\";\"5100\";\"23-09-1991\";\"0123456789\";\"0303501111\";\"1316\";\"955\";\"9507\";\"\";\"0\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );
    }


    @Test
    public void testFileOutput() throws IOException {
        civilStatusDataService.setWriteToLocalFile(true);
        ResponseEntity<String> response = restTemplate.exchange("/statistik/civilstate_data/?CivSt=G&registrationAfter=1980-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/civilstate_data/?CivSt=G&registrationAfter=1980-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);

        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNull(response.getBody());

        String[] marriedFiles = new File(StatisticsService.PATH_FILE).list((dir, name) -> name.startsWith("civilstatus"));
        Assert.assertEquals(1, marriedFiles.length);

        FileInputStream fileInputStream = new FileInputStream(StatisticsService.PATH_FILE + File.separator + marriedFiles[0]);
        String contents = InputStreamReader.readInputStream(
                fileInputStream,"UTF-8"
        );
        fileInputStream.close();

        String expected = "\"CivSt\";\"CivDto\";\"StatKod\";\"ProdDto\";\"Pnr\";\"AegtePnr\";\"authority\";\"KomKod\";\"FoedMynKod\";\"FoedMynTxt\";\"FoedMynKodTxt\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"G\";\"05-10-1990\";;\"22-05-1989\";\"0123456789\";\"0202501111\";\"1316\";;\"9507\";\"\";\"0\";;;;;;;;\n" +
                "\"G\";\"01-03-1998\";\"5100\";\"23-09-1991\";\"0123456789\";\"0303501111\";\"1316\";\"955\";\"9507\";\"\";\"0\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(contents.trim())
        );
    }
}
