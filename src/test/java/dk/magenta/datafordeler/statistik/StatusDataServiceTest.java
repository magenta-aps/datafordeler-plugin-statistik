package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import dk.magenta.datafordeler.statistik.services.StatusDataService;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class StatusDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    @Test
    public void testStatusDataService() throws Exception {
        StatisticsService.isFileOn = false;

        testsUtils.loadPersonData("statusperson.txt");
        testsUtils.loadGladdrregData();
        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/status_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/status_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());

        System.out.println("Body response: "+response.getBody());
        Assert.assertNotNull(response.getBody());
        Assert.assertFalse(response.getBody().isEmpty());

        Assert.assertEquals(
                "\"Pnr\";\"FoedAar\";\"Fornavn\";\"Efternavn\";\"Status\";\"FoedMynKod\";\"StatKod\";\"M_Pnr\";\"F_Pnr\";\"CivSt\";\"AegtePnr\";\"KomKod\";\"LokNavn\";\"LokKode\";\"LokKortNavn\";\"VejKod\";\"HusNr\";\"Etage\";\"SideDoer\";\"Bnr\";\"TilFlyDto\";\"FlytProdDto\";\"Postnr\";\"CivDto\";\"CivProdDto\";\"Kirke\"\n" +
                        "\"0101001234\";\"2000\";\"Tester Testmember\";\"Testersen\";\"05\";\"9516\";\"5100\";\"2903641234\";\"0101641234\";\"G\";\"0202994321\";\"955\";\"Paamiut\";\"0500\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"30-08-2016\";\"31-08-2016\";\"3982\";\"12-10-2017\";\"13-10-2017\";\"F\"",
                response.getBody().trim()
        );
    }
}
