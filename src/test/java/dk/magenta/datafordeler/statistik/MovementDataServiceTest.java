package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.databind.ObjectMapper;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.statistik.services.MovementDataService;
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

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private TestUtil testUtil;

    @Autowired
    private ObjectMapper objectMapper;

    /*@Before
    public void initialize() throws Exception {
        //testsUtils.loadPersonData(new File("/home/lars/tmp/download/tmp.txt"));
        //testsUtils.loadPersonData(new File("/home/lars/tmp/download/1608116792.txt"));
    }*/

    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadGladdrregData();
        testsUtils.loadPersonData("movedperson.txt");
    }

    @After
    public void cleanup() {
        System.out.println("Cleaning up");
        testsUtils.clearPath();
        //testsUtils.deleteAll();
    }

    @Test
    public void testService() throws Exception {
        movementDataService.setWriteToLocalFile(false);
        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/movement_data/?registrationAfter=2018-01-01", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNotNull(response.getBody());

        System.out.println(response.getBody());

        Assert.assertFalse(response.getBody().isEmpty());
        String expected = "\"Pnr\";\"FoedAar\";\"PnrGaeld\";\"Status\";\"FoedMynKod\";\"StatKod\";\"M_Pnr\";\"F_Pnr\";\"AegtePnr\";\"ProdDto\";\"FlyDto\";\"FraLand\";\"FraKomKod\";\"FraLokKortNavn\";\"FraVejKod\";\"FraHusNr\";\"FraEtage\";\"FraSideDoer\";\"FraBnr\";\"TilLand\";\"TilKomKod\";\"TilLokKortNavn\";\"TilVejKod\";\"TilHusNr\";\"TilEtage\";\"TilSideDoer\";\"TilBnr\"\n" +
                "\"0101001234\";\"2000\";;\"05\";\"9516\";;\"2903641234\";\"0101641234\";\"1012291422\";\"01-03-2018\";\"01-03-2012\";\"5390\";;;;;;;;;\"955\";\"PAA\";\"0001\";\"0002\";\"\";\"\";\"5678\"";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );
    }

    @Test
    public void testFileOutput() throws IOException {
        movementDataService.setWriteToLocalFile(true);

        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/movement_data/?registrationAfter=2016-01-01", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNull(response.getBody());

        Assert.assertEquals(200, response.getStatusCodeValue());

        String[] movementFiles = new File(StatisticsService.PATH_FILE).list((dir, name) -> name.startsWith("movement"));
        Assert.assertEquals(1, movementFiles.length);

        FileInputStream fileInputStream = new FileInputStream(StatisticsService.PATH_FILE + File.separator + movementFiles[0]);
        String contents = InputStreamReader.readInputStream(
                fileInputStream,"UTF-8"
        );

        String expected = "\"Pnr\";\"FoedAar\";\"PnrGaeld\";\"Status\";\"FoedMynKod\";\"StatKod\";\"M_Pnr\";\"F_Pnr\";\"AegtePnr\";\"ProdDto\";\"FlyDto\";\"FraLand\";\"FraKomKod\";\"FraLokKortNavn\";\"FraVejKod\";\"FraHusNr\";\"FraEtage\";\"FraSideDoer\";\"FraBnr\";\"TilLand\";\"TilKomKod\";\"TilLokKortNavn\";\"TilVejKod\";\"TilHusNr\";\"TilEtage\";\"TilSideDoer\";\"TilBnr\"\n" +
                        "\"0101001234\";\"2000\";;\"05\";\"9516\";;\"2903641234\";\"0101641234\";\"1012291422\";\"01-03-2018\";\"01-03-2012\";\"5390\";;;;;;;;;\"955\";\"PAA\";\"0001\";\"0002\";\"\";\"\";\"5678\"";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(contents.trim())
        );
    }

}
