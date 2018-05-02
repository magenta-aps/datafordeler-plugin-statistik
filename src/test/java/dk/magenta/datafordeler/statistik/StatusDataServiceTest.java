package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.apache.commons.io.FilenameUtils;
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class StatusDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.loadPersonData("statusperson.txt");
        testsUtils.loadGladdrregData();

        //Use this code block when temp directories need to be created
        Path path = Files.createTempDirectory("statistik");
        StatisticsService.PATH_FILE = String.valueOf(path);

    }

    @After
    public void cleanup() {
        testsUtils.deleteAll();
    }



    @Test
    public void testStatusDataService() {
        StatisticsService.isFileOn = false;

        ResponseEntity<String> response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNotNull("Response contains a body", response);
      /*  Assert.assertEquals(
                "\"Pnr\";\"FoedAar\";\"Fornavn\";\"Efternavn\";\"Status\";\"FoedMynKod\";\"FoedMynKodTxt\";\"StatKod\";\"M_Pnr\";\"F_Pnr\";\"CivSt\";\"AegtePnr\";\"KomKod\";\"LokNavn\";\"LokKode\";\"LokKortNavn\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\";\"TilFlyDto\";\"FlytProdDto\";\"Postnr\";\"CivDto\";\"CivProdDto\";\"Kirke\"\n" +
                        "\"0101001234\";\"2000\";\"Tester Testmember\";\"Testersen\";\"05\";\"9516\";\"0\";\"5100\";\"2903641234\";\"0101641234\";\"G\";\"0202994321\";\"955\";\"Paamiut\";\"0500\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"30-08-2016\";\"31-08-2016\";\"3982\";\"12-10-2017\";\"13-10-2017\";\"F\"",
                response.getBody().trim()
        );*/
        System.out.println("Body response: "+response.getBody());
    }

    @Test
    public void testDirectoryFileCreationDeletion() {
        StatisticsService.isFileOn = true;

        ResponseEntity<String> response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);

        testsUtils.deleteFiles(StatisticsService.PATH_FILE);

    }




}
