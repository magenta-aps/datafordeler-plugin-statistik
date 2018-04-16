package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;

import dk.magenta.datafordeler.statistik.services.DeathDataService;
import org.junit.Test;
import static org.junit.Assert.assertThat;
//import org.hamcrest.core.Is.is;



import static org.hamcrest.CoreMatchers.*;
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
public class DeathDataServiceTest {

    private DeathDataService deathDataService;
    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private PersonEntityManager personEntityManager;




    @Test
    public void testDeathDataService()throws Exception {
        PersonTestsUtils testsUtils = new PersonTestsUtils(sessionManager, personEntityManager);
        testsUtils.loadPersonData();
        //loadPerson();
        HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/123?effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        assertThat(response.getBody(), is(not("")));

        //assertThat(testsUtils, is(not("")));

        System.out.println("Body response: "+response.getBody());
    }

}
