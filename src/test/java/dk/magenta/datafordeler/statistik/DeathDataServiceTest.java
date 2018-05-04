package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.apache.commons.io.FileUtils;
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

import static org.junit.Assert.assertNotNull;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DeathDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.loadPersonData("deadperson.txt");
        testsUtils.loadGladdrregData();

        //Use this code block when temp directories need to be created
        /*Path path = Files.createTempDirectory("statistik");
        StatisticsService.PATH_FILE = String.valueOf(path);*/
    }

    @After
    public void cleanup() {
        testsUtils.deleteAll();
    }


    @Test
    public void testDeathDataService() {
        StatisticsService.isFileOn = false;

        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/?afterDate=1817-07-01&beforeDate=2049-09-30&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/death_data/?afterDate=1817-07-01&beforeDate=2049-09-30&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response contains a body", response);
     /*   Assert.assertEquals(
                "\"Status\";\"DoedDto\";\"ProdDto\";\"Pnr\";\"FoedAar\";\"M_Pnr\";\"F_Pnr\";\"AegtePnr\";\"PnrGaeld\";\"StatKod\";\"FoedMynKod\";\"FoedMynKodTxt\";\"KomKod\";\"LokNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"SideDoer\";\"Bnr\"\n" +
                        "\"90\";\"30-08-2017\";\"31-08-2017\";\"0101501234\";\"2000\";\"2903641234\";\"0101641234\";\"0202994321\";\"\";;\"9516\";\"0\";\"955\";\"Paamiut\";\"0500\";\"0001\";\"0005\";\"tv\";\"1234\"",
                response.getBody().trim()
        );*/
        System.out.println("Body response: "+response.getBody());
    }


    @Test
    public void testFileOutput() {
        StatisticsService.isFileOn = true;
        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/?afterDate=1817-07-01&beforeDate=2049-09-30&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/death_data/?afterDate=1817-07-01&beforeDate=2049-09-30&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);

        testsUtils.deleteFiles(StatisticsService.PATH_FILE);


    }

}
