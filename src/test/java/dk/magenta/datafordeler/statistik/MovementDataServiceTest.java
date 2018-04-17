package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.statistik.services.DeathDataService;
import dk.magenta.datafordeler.statistik.services.MovementDataService;
import dk.magenta.datafordeler.statistik.services.StatusDataService;
import org.hamcrest.CoreMatchers;
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
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.CoreMatchers.not;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)

public class MovementDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;


    @Test
    public void testMovementDataService()throws Exception {
        testsUtils.loadPersonData("movedperson.txt");
        HttpEntity<String> httpEntity = new HttpEntity<>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/movement_data/?effectDate=2016-09-01&afterDate=2000-08-01&beforeDate=2020-09-01", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNotNull(response.getBody());
        Assert.assertFalse(response.getBody().isEmpty());
        System.out.println("Body response: "+response.getBody());
    }
}
