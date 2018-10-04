package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.core.JsonProcessingException;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import dk.magenta.datafordeler.statistik.services.StatusDataService;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class StatusDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    @Autowired
    private StatusDataService statusDataService;

    TestUserDetails testUserDetails;

    @Autowired
    private TestUtil testUtil;

    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadPersonData("statusperson.txt");
        testsUtils.loadGladdrregData();
    }

    @After
    public void cleanup() {
        testsUtils.clearPath();
        testsUtils.deleteAll();
    }

    @Test
    public void testStatusDataService() throws JsonProcessingException {
        statusDataService.setWriteToLocalFile(false);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNotNull("Response contains a body", response);

        String expected = "\"Pnr\";\"FoedAar\";\"Fornavn\";\"Efternavn\";\"Status\";\"FoedMynKod\";\"FoedMynTxt\";\"StatKod\";\"M_Pnr\";\"F_Pnr\";\"CivSt\";\"AegtePnr\";\"KomKod\";\"LokNavn\";\"LokKode\";\"LokKortNavn\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\";\"TilFlyDto\";\"FlytProdDto\";\"Postnr\";\"CivDto\";\"CivProdDto\";\"Kirke\"\n" +
                "\"0101001234\";\"2000\";\"Tester Testmember\";\"Testersen\";\"05\";\"9516\";\"\";\"5100\";\"2903641234\";\"0101641234\";\"G\";\"0202994321\";\"955\";\"Paamiut\";\"0500\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"30-08-2016\";\"31-08-2016\";\"3982\";\"12-10-2017\";\"13-10-2017\";\"F\"";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );
    }

    @Test
    public void testFileOutput() throws IOException {
        statusDataService.setWriteToLocalFile(true);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-05-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);

        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNull(response.getBody());

        String[] statusFiles = new File(StatisticsService.PATH_FILE).list((dir, name) -> name.startsWith("status"));
        Assert.assertEquals(1, statusFiles.length);

        FileInputStream fileInputStream = new FileInputStream(StatisticsService.PATH_FILE + File.separator + statusFiles[0]);
        String contents = InputStreamReader.readInputStream(
                fileInputStream,"UTF-8"
        );
        fileInputStream.close();

        String expected = "\"Pnr\";\"FoedAar\";\"Fornavn\";\"Efternavn\";\"Status\";\"FoedMynKod\";\"FoedMynTxt\";\"StatKod\";\"M_Pnr\";\"F_Pnr\";\"CivSt\";\"AegtePnr\";\"KomKod\";\"LokNavn\";\"LokKode\";\"LokKortNavn\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\";\"TilFlyDto\";\"FlytProdDto\";\"Postnr\";\"CivDto\";\"CivProdDto\";\"Kirke\"\n" +
                        "\"0101001234\";\"2000\";\"Tester Testmember\";\"Testersen\";\"05\";\"9516\";\"\";\"5100\";\"2903641234\";\"0101641234\";\"G\";\"0202994321\";\"955\";\"Paamiut\";\"0500\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"30-08-2016\";\"31-08-2016\";\"3982\";\"12-10-2017\";\"13-10-2017\";\"F\"";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(contents.trim())
        );
    }

}
