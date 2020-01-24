package dk.magenta.datafordeler.statistik;

import com.fasterxml.jackson.core.JsonProcessingException;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.AdoptionDataService;
import org.json.JSONException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.skyscreamer.jsonassert.JSONAssert;
import org.skyscreamer.jsonassert.JSONCompareMode;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@DirtiesContext(classMode = DirtiesContext.ClassMode.BEFORE_EACH_TEST_METHOD)
public class AdoptionDataServiceTest extends TestBase {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    TestUserDetails testUserDetails;

    @Autowired
    private TestUtil testUtil;

    @Autowired
    private AdoptionDataService adoptionDataService;

    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadPersonData("adoptedpersons.txt");
        this.loadAllGeoAdress(sessionManager);
        adoptionDataService.setUseTimeintervallimit(false);
    }

    @After
    public void cleanup() {
        testsUtils.clearPath();
        testsUtils.deleteAll();
    }

    @Test
    public void testService() throws JsonProcessingException, JSONException {
        adoptionDataService.setWriteToLocalFile(false);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/adoption_data/", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testUserDetails.giveAccess(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/adoption_data/?registrationAfter=2000-01-01", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());

        Assert.assertNotNull("Response contains a body", response);
        String expected = "\"CODE\";\"Pnr\";\"FoedAar\";\"M_Pnr\";\"F_Pnr\";\"AM_mynkod\";\"AF_mynkod\";\"FoedMynKod\";\"StatKod\";\"ProdDto\";\"ProdFilDto\";\"AdoptionDto\";\"KomKod\";\"LokNavn\";\"LokKortNavn\";\"LokKode\";\"VejKod\";\"HusNr\";\"Etage\";\"Bnr\"\n" +
                "\"POST\";\"1111111112\";\"2004\";\"2222222222\";\"2222222223\";\"99\";\"1316\";\"9507\";\"5100\";\"18-12-2019\";;\"18-12-2019\";\"956\";\"Nuuk\";\"NUK\";\"0600\";\"0254\";\"0018\";\"1\";\"1234\"\n" +
                "\"PRE\";\"1111111112\";\"2004\";\"2222222222\";\"2222222225\";\"99\";\"1202\";\"9507\";\"5100\";\"18-12-2019\";;\"18-12-2019\";\"956\";\"Nuuk\";\"NUK\";\"0600\";\"0254\";\"0018\";\"1\";\"1234\"\n" +
                "\"POST\";\"1111111111\";\"2004\";\"2222222226\";\"2222222227\";\"1350\";\"1350\";\"9507\";\"5100\";\"04-12-2019\";;\"04-12-2019\";\"956\";\"Nuuk\";\"NUK\";\"0600\";\"0254\";\"0018\";\"1\";\"1234\"\n" +
                "\"PRE\";\"1111111111\";\"2004\";\"2222222228\";\"2222222229\";\"99\";\"1202\";\"9507\";\"5100\";\"04-12-2019\";;\"04-12-2019\";\"956\";\"Nuuk\";\"NUK\";\"0600\";\"0254\";\"0018\";\"1\";\"1234\"";

        JSONAssert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim()),
                        JSONCompareMode.LENIENT
        );
    }


}
