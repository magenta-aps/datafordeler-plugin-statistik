package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.DeathDataService;
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
import java.io.IOException;

import static org.junit.Assert.assertNotNull;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DeathDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    @Autowired
    private DeathDataService deathDataService;

    @Autowired
    private TestUtil testUtil;

    @Autowired
    private ObjectMapper objectMapper;

    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadPersonData("deadperson.txt");
        testsUtils.loadGladdrregData();
    }

    @After
    public void cleanup() {
        testsUtils.clearPath();
        testsUtils.deleteAll();
    }

    @Test
    public void testService() throws JsonProcessingException {
        deathDataService.setWriteToLocalFile(false);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/?registrationAfter=2017-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/death_data/?registrationAfter=2000-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response contains a body", response);
        String expected =  "\"Status\";\"DoedDto\";\"ProdDto\";\"ProdFilDto\";\"Pnr\";\"CivSt\";\"FoedAar\";\"M_Pnr\";\"F_Pnr\";\"AegtePnr\";\"PnrGaeld\";\"StatKod\";\"FoedMynKod\";\"FoedMynTxt\";\"KomKod\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"90\";\"30-08-2017\";\"31-08-2017\";\"\";\"0101501234\";\"\";\"2000\";\"2903641234\";\"0101641234\";;;;\"9516\";\"\";\"955\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );
    }


    @Test
    public void testFileOutput() throws IOException {
        deathDataService.setWriteToLocalFile(true);
        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/?registrationAfter=2017-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/death_data/?registrationAfter=2017-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);

        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNull(response.getBody());

        String[] deathFiles = new File(StatisticsService.PATH_FILE).list((dir, name) -> name.startsWith("death"));
        Assert.assertEquals(1, deathFiles.length);

        FileInputStream fileInputStream = new FileInputStream(StatisticsService.PATH_FILE + File.separator + deathFiles[0]);
        String contents = InputStreamReader.readInputStream(
                fileInputStream,"UTF-8"
        );
        fileInputStream.close();

        String expected = "\"Status\";\"DoedDto\";\"ProdDto\";\"ProdFilDto\";\"Pnr\";\"CivSt\";\"FoedAar\";\"M_Pnr\";\"F_Pnr\";\"AegtePnr\";\"PnrGaeld\";\"StatKod\";\"FoedMynKod\";\"FoedMynTxt\";\"KomKod\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\"\n" +
                "\"90\";\"30-08-2017\";\"31-08-2017\";\"\";\"0101501234\";\"\";\"2000\";\"2903641234\";\"0101641234\";;;;\"9516\";\"\";\"955\";\"Paamiut\";\"PAA\";\"0500\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(contents.trim())
        );

    }

}
