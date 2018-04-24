package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.statistik.services.DeathDataService;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.apache.commons.io.FilenameUtils;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.containsString;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

//import org.hamcrest.core.Is.is;


@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class DeathDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    @Autowired
    private DeathDataService deathDataService;

    @Test
    public void testDeathDataService() throws Exception {
        StatisticsService.isFileOn = false;
        testsUtils.loadPersonData("deadperson.txt");
        testsUtils.loadGladdrregData();
        TestUserDetails testUserDetails = new TestUserDetails();

        HttpEntity<String> httpEntity = new HttpEntity<String>("", new HttpHeaders());
        ResponseEntity<String> response = restTemplate.exchange("/statistik/death_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/death_data/", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(400, response.getStatusCodeValue());

        response = restTemplate.exchange("/statistik/death_data/?afterDate=1817-07-01&beforeDate=2049-09-30&effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        Assert.assertNotEquals("", response.getBody());
        System.out.println("Body response: "+response.getBody());
    }


    @Test
    public void testDirectoryCreation(){
        // StatisticsService.isFileOn = true;
        //Directory and file creation
        File folder = new File(System.getProperty("user.home") + File.separator + "statistik");
        assertTrue(folder.exists());

        //Checking all files in folder have content
        File[] listOfFiles = folder.listFiles();
        for (File file : listOfFiles)
            if (file.isFile()) {
                System.out.println(file.getName());

                assertTrue(file.length() > 0);
                String basename = FilenameUtils.getBaseName(file.getName());
                String extension = FilenameUtils.getExtension(file.getName());

                assertThat(basename, containsString("death"));
                assertThat(extension, is("csv"));

                String content;
                try {
                    content =  new String (Files.readAllBytes(Paths.get(folder + File.separator +file.getName())));
                    Assert.assertEquals(
                            "Status;DoedDto;ProdDto;Pnr;FoedAar;M_Pnr;F_Pnr;AegtePnr;PnrGaeld;StatKod;FoedMynKod;KomKod;LokNavn;LokKode;VejKod;HusNr;SideDoer;Bnr\n" +
                                    "90;\"30-08-2017\";\"31-08-2017\";\"0101501234\";2000;\"2903641234\";\"0101641234\";\"0202994321\";;;0;955;;;\"0001\";\"0005\";tv;\"1234\""
                            , content.trim()
                    );


                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
    }


}
