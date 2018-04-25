package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import dk.magenta.datafordeler.statistik.services.StatusDataService;
import org.apache.commons.io.FilenameUtils;
import org.junit.Assert;
import org.junit.Before;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

import static junit.framework.TestCase.assertTrue;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class StatusDataServiceTest {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    HttpEntity<String> httpEntity;
    ResponseEntity<String> response;
    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.loadPersonData("statusperson.txt");
        testsUtils.loadGladdrregData();

        testUserDetails = new TestUserDetails();
        httpEntity = new HttpEntity<String>("", new HttpHeaders());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = null;
    }



    @Test
    public void testStatusDataService() {
        StatisticsService.isFileOn = false;
        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        System.out.println("Body response: "+response.getBody());

    }


    @Test
    public void testStatusFileExistenceAndContent(){
        StatisticsService.isFileOn = true;
        response = restTemplate.exchange("/statistik/status_data/?effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        System.out.println("Body response: "+response.getBody());

        //Directory and file creation
        File folder = new File(System.getProperty("user.home") + File.separator + "statistik");
        if(folder.exists()){
            //Checking all files in folder have content
            File[] listOfFiles = folder.listFiles();
            if(listOfFiles.length > 0) {
                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        assertTrue(file.length() > 0);
                        String basename = FilenameUtils.getBaseName(file.getName());
                        String extension = FilenameUtils.getExtension(file.getName());

                        if(basename.contains(StatisticsService.ServiceName.STATUS.name().toLowerCase())){
                            String content;
                            try {
                                content = new String(Files.readAllBytes(Paths.get(folder + File.separator + file.getName())));
                                assertThat(extension, is("csv"));
                                Assert.assertEquals(
                                        "Pnr;FoedAar;Fornavn;Efternavn;Status;FoedMynKod;StatKod;M_Pnr;F_Pnr;CivSt;AegtePnr;KomKod;LokNavn;LokKode;LokKortNavn;VejKod;HusNr;Etage;SideDoer;Bnr;TilFlyDto;FlytProdDto;Postnr;CivDto;CivProdDto;Kirke\n" +
                                                "\"0101001234\";2000;\"Tester Testmember\";Testersen;\"05\";9516;5100;\"2903641234\";\"0101641234\";G;\"0202994321\";955;;;;\"0001\";\"0005\";\"1\";tv;\"1234\";\"30-08-2016\";\"31-08-2016\";0;\"12-10-2017\";\"13-10-2017\";F",

                                        content.trim()

                                );
                                System.out.println(file.getName()+" file process correctly.");

                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        }else{
                            System.out.println(file.getName()+" was not process in this test.");
                        }


                    }
                }

            }

        }else{
            System.out.println("Folder do not exist.");
        }

    }



}
