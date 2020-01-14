package dk.magenta.datafordeler.statistik;


import com.fasterxml.jackson.core.JsonProcessingException;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.LocalityDataService;
import dk.magenta.datafordeler.statistik.services.RoadDataService;
import org.junit.Assert;
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

import static org.junit.Assert.assertNotNull;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class LocalityDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    @Autowired
    private LocalityDataService localityDataService;

    @Autowired
    private TestUtil testUtil;

    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadGeoLocalityData("Lokalitet_test.json");
    }

    @Test
    public void testDummy() {
    }

    //@Test
    public void testService() throws JsonProcessingException {
        localityDataService.setWriteToLocalFile(false);

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        MultiValueMap<String,Object> form = new LinkedMultiValueMap<String,Object>();
        form.add("file", new InputStreamResource(LocalityDataServiceTest.class.getResourceAsStream("/addressInput.csv")));

        ResponseEntity<String> response = restTemplate.exchange("/statistik/locality_data/", HttpMethod.GET, new HttpEntity(form, new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        assertNotNull("Response contains a body", response);
        String expected = "\"KomKod\";\"KomKortNavn\";\"KomNavn\";\"LokKode\";\"LokKortNavn\";\"LokNavn\";\"LokTypeKod\";\"LokTypeNavn\";\"LokStatusKod\";\"LokStatusNavn\";\"Void\"\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1700\";\"QNQ\";\"Qaanaaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1706\";\"MOR\";\"Moriusaq\";\"4\";\"Nedlagt bygd\";\"20\";\"Nedlagt\";";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );



    }
}