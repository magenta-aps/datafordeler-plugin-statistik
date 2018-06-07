package dk.magenta.datafordeler.statistik;


import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
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

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AddressDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    TestUserDetails testUserDetails;
    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadPersonData("bornperson.txt");
        testsUtils.loadGladdrregData();
    }

    @Test
    public void testService() {
        StatisticsService.isFileOn = true;

        ResponseEntity<String> response = restTemplate.exchange("/statistik/address_data/", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        //Assert.assertEquals(500, response.getStatusCodeValue());
        System.out.println("Response is: "+response.toString());

    }
}