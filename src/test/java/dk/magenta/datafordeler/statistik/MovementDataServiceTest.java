package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.MovementDataService;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.junit.Assert;
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

    @Test
    public void testMovementDataService() throws Exception {
        StatisticsService.isFileOn = false;
        testsUtils.loadPersonData("movedperson.txt");
        testsUtils.loadGladdrregData();
        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/movement_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/movement_data/?effectDate=2016-09-01&afterDate=2000-08-01&beforeDate=2020-09-01", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNotNull(response.getBody());
        Assert.assertFalse(response.getBody().isEmpty());
        System.out.println("Body response: "+response.getBody());
    }







}
