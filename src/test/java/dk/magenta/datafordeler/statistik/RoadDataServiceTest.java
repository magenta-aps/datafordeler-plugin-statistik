package dk.magenta.datafordeler.statistik;


import com.fasterxml.jackson.core.JsonProcessingException;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.RoadDataService;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.core.io.InputStreamResource;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.ContextConfiguration;
import org.springframework.test.context.junit4.SpringJUnit4ClassRunner;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class RoadDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    @Autowired
    private RoadDataService roadDataService;

    @Autowired
    private TestUtil testUtil;

    TestUserDetails testUserDetails;

    //WE have no testdata and this is not considered ar real unittest
    //@Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadGeoLocalityData("Lokalitet_test.json");
        testsUtils.loadGeoRoadData("Vejmidte_test.json");
        testsUtils.loadAccessLocalityData("Adgangsadresse_test.json");
        testsUtils.loadPostalLocalityData("Postnummer_test.json");

    }

    @Test
    public void testDummy() {
    }

    //WE have no testdata and this is not considered ar real unittest
    //@Test
    public void testService() throws JsonProcessingException {
        roadDataService.setWriteToLocalFile(false);

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        MultiValueMap<String,Object> form = new LinkedMultiValueMap<String,Object>();
        form.add("file", new InputStreamResource(RoadDataServiceTest.class.getResourceAsStream("/addressInput.csv")));

        ResponseEntity<String> response = restTemplate.exchange("/statistik/road_data/", HttpMethod.POST, new HttpEntity(form, new HttpHeaders()), String.class);

        System.out.println(response.toString());

    }
}