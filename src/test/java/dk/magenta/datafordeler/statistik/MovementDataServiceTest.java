package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.MovementDataService;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
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
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class MovementDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    @Autowired
    private MovementDataService movementDataService;

    @Before
    public void initialize() throws Exception {
        testsUtils.loadPersonData("movedperson.txt");
        testsUtils.loadGladdrregData();

        //Use this code block when temp directories need to be created
        Path path = Files.createTempDirectory("statistik");
        StatisticsService.PATH_FILE = String.valueOf(path);
    }

    @After
    public void cleanup() {

        testsUtils.deleteFiles(StatisticsService.PATH_FILE);
        testsUtils.deleteAll();
    }

    @Test
    public void testMovementDataService() throws Exception {
        StatisticsService.isFileOn = false;
        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/movement_data/?effectDate=2012-01-02&afterDate=2012-01-01&beforeDate=2012-04-01", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNotNull(response.getBody());
        Assert.assertFalse(response.getBody().isEmpty());
        System.out.println("Body response: "+response.getBody());
    }

    @Test
    public void testFileOutput() throws IOException {
        StatisticsService.isFileOn = true;

        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/movement_data/?effectDate=2012-01-02&afterDate=2012-01-01&beforeDate=2012-04-01", HttpMethod.GET, httpEntity, String.class);

        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNull(response.getBody());

        String[] birthFiles = new File(StatisticsService.PATH_FILE).list((dir, name) -> name.startsWith("movement"));
        Assert.assertEquals(1, birthFiles.length);

        FileInputStream fileInputStream = new FileInputStream(StatisticsService.PATH_FILE + File.separator + birthFiles[0]);
        String contents = InputStreamReader.readInputStream(
                fileInputStream,"UTF-8"
        );
        fileInputStream.close();

        Assert.assertEquals(
                "\"B_Pnr\";\"B_FoedAar\";\"B_PnrGaeld\";\"B_FoedMynKod\";\"B_FoedMynKodTxt\";\"B_StatKod\";\"B_ProdDto\";\"M_Pnr\";\"M_FoedMynKod\";\"M_FoedMynKodTxt\";\"M_StatKod\";\"M_KomKod\";\"M_LokNavn\";\"M_LokKode\";\"M_VejKod\";\"M_HusNr\";\"M_Etage\";\"M_SideDoer\";\"M_Bnr\";\"F_Pnr\";\"F_FoedMynKod\";\"F_FoedMynKodTxt\";\"F_StatKod\";\"F_KomKod\";\"F_LokNavn\";\"F_LokKode\";\"F_VejKod\";\"F_HusNr\";\"F_Etage\";\"F_SideDoer\";\"F_Bnr\"\n" +
                        "\"0101001234\";\"2000\";;\"9516\";\"0\";\"5100\";\"13-01-2000\";\"2903641234\";\"6666\";\"0\";\"5100\";\"955\";\"Paamiut\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"0101641234\";\"8888\";\"0\";\"5100\";\"955\";\"Paamiut\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"",
                contents.trim()
        );
    }

}
