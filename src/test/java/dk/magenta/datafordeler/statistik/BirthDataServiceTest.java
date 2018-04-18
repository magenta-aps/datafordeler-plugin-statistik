package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
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
public class BirthDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    @Test
    public void testBirthDataService() throws Exception {
        testsUtils.loadPersonData("person.txt");
        testsUtils.loadGladdrregData();

        TestUserDetails testUserDetails = new TestUserDetails();
        HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/birth_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        System.out.println("Body response: "+response.getBody());
    }

}
