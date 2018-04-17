package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
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

//import org.hamcrest.core.Is.is;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DeathDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private PersonEntityManager personEntityManager;

    @Autowired
    private PersonTestsUtils testsUtils;

    @Test
    public void testDeathDataService()throws Exception {
        testsUtils.loadPersonData("deadperson.txt");
        testsUtils.loadGladdrregData();
        HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/?afterDate=2017-07-01&beforeDate=2017-09-30&effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNotEquals("", response.getBody());
        System.out.println("Body response: "+response.getBody());
    }

}
