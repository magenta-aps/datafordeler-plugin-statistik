package dk.magenta.datafordeler.statistik;

import static com.jcabi.matchers.RegexMatchers.matchesPattern;
import jdk.nashorn.internal.runtime.regexp.RegExpMatcher;
import org.apache.commons.io.FilenameUtils;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.prefs.AbstractPreferences;

import static junit.framework.TestCase.assertTrue;
import static org.apache.commons.lang3.Validate.matchesPattern;
import static org.hamcrest.CoreMatchers.*;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BirthDataServiceTest  {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    HttpEntity<String> httpEntity;
    ResponseEntity<String> response;
    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.loadPersonData("person.txt");
        testsUtils.loadGladdrregData();

        testUserDetails = new TestUserDetails();
        httpEntity = new HttpEntity<String>("", new HttpHeaders());

        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = null;
    }

    @Test
    public void testBirthDataService(){
        StatisticsService.isFileOn = false;
        response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        System.out.println("Body response: "+response.getBody());

    }

    @Test
    public void testBirthFileExistenceAndContent(){

        StatisticsService.isFileOn = true;
        response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, httpEntity, String.class);
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

                        if(basename.contains(StatisticsService.ServiceName.BIRTH.name().toLowerCase())){
                            String content;
                            try {
                                content = new String(Files.readAllBytes(Paths.get(folder + File.separator + file.getName())));
                                assertThat(extension, is("csv"));
                                Assert.assertEquals("B_Pnr;B_FoedAar;B_PnrGaeld;B_FoedMynKod;B_StatKod;B_ProdDto;M_Pnr;M_FoedMynKod;M_StatKod;M_KomKod;M_LokNavn;M_LokKode;M_VejKod;M_HusNr;M_SideDoer;M_Bnr;F_Pnr;F_FoedMynKod;F_StatKod;F_KomKod;F_LokNavn;F_LokKode;F_VejKod;F_HusNr;F_SideDoer;F_Bnr\n" +
                                                "\"0101001234\";2000;;9516;5100;\"13-01-2000\";\"0101641234\";6666;;955;;;\"0001\";\"0005\";tv;\"1234\";;8888;;955;;;\"0001\";\"0005\";tv;\"1234\""
                                        , content.trim()
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
