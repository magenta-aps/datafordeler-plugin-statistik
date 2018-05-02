package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.junit.After;
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

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class BirthDataServiceTest  {

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private PersonTestsUtils testsUtils;

    TestUserDetails testUserDetails;

    @Before
    public void initialize() throws Exception {
        testsUtils.loadPersonData("person.txt");
        testsUtils.loadGladdrregData();

        //Use this code block when local directories need to be created
      /*  StatisticsService.PATH_FILE = System.getProperty("user.home") + File.separator + "statistik";
        File folder = new File( StatisticsService.PATH_FILE);
        if (!folder.exists()) {
            folder.mkdirs();
        }*/


        //Use this code block when temp directories need to be created
        Path path = Files.createTempDirectory("statistik");
        StatisticsService.PATH_FILE = String.valueOf(path);
    }

    @After
    public void cleanup() {
        testsUtils.deleteAll();
    }

    @Test
    public void testBirthDataService(){
        StatisticsService.isFileOn = false;

        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());
        
        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(200, response.getStatusCodeValue());
        Assert.assertNotNull("Response contains a body", response);
        /*Assert.assertEquals("\"B_Pnr\";\"B_FoedAar\";\"B_PnrGaeld\";\"B_FoedMynKod\";\"B_FoedMynKodTxt\";\"B_StatKod\";\"B_ProdDto\";\"M_Pnr\";\"M_FoedMynKod\";\"M_FoedMynKodTxt\";\"M_StatKod\";\"M_KomKod\";\"M_LokNavn\";\"M_LokKode\";\"M_VejKod\";\"M_HusNr\";\"M_Etage\";\"M_SideDoer\";\"M_Bnr\";\"F_Pnr\";\"F_FoedMynKod\";\"F_FoedMynKodTxt\";\"F_StatKod\";\"F_KomKod\";\"F_LokNavn\";\"F_LokKode\";\"F_VejKod\";\"F_HusNr\";\"F_Etage\";\"F_SideDoer\";\"F_Bnr\"\n" +
                        "\"0101001234\";\"2000\";\"\";\"9516\";\"0\";\"5100\";\"13-01-2000\";\"2903641234\";\"6666\";\"0\";\"5100\";\"955\";\"Paamiut\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"0101641234\";\"8888\";\"0\";\"5100\";\"955\";\"Paamiut\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"",
                response.getBody().trim()
        );*/
        /*Assert.assertEquals("\"B_Pnr\";\"B_FoedAar\";\"B_PnrGaeld\";\"B_FoedMynKod\";\"B_FoedMynKodTxt\";\"B_StatKod\";\"B_ProdDto\";\"M_Pnr\";\"M_FoedMynKod\";\"M_FoedMynKodTxt\";\"M_StatKod\";\"M_KomKod\";\"M_LokNavn\";\"M_LokKode\";\"M_VejKod\";\"M_HusNr\";\"M_Etage\";\"M_SideDoer\";\"M_Bnr\";\"F_Pnr\";\"F_FoedMynKod\";\"F_FoedMynKodTxt\";\"F_StatKod\";\"F_KomKod\";\"F_LokNavn\";\"F_LokKode\";\"F_VejKod\";\"F_HusNr\";\"F_Etage\";\"F_SideDoer\";\"F_Bnr\"\n" +
                        "\"0101001234\";\"2000\";\"\";\"9516\";\"0\";\"5100\";\"13-01-2000\";\"2903641234\";\"6666\";\"0\";\"5100\";\"955\";;;\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"0101641234\";\"8888\";\"0\";\"5100\";\"955\";;;\"0001\";\"0005\";\"1\";\"tv\";\"1234\"",
                response.getBody().trim()
        );*/
        System.out.println("Body response: "+response.getBody());
    }


    @Test
    public void testDirectoryFile_CreationDeletion() {
        StatisticsService.isFileOn = true;

        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        Assert.assertEquals(403, response.getStatusCodeValue());

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);

            //This code can be places in @After
            File tempDir = null;
            try{
                tempDir = new File(StatisticsService.PATH_FILE);
               boolean exists = tempDir.exists();
               Assert.assertEquals(true, exists);
           }catch(Exception e) {
               // if any error occurs
               e.printStackTrace();
           }finally {
               File[] listOfFiles = tempDir.listFiles();
               if (listOfFiles.length > 0) {

                   System.out.println("Number of files to delete: "+ listOfFiles.length);
                   for (File file : listOfFiles) {
                       file.deleteOnExit();
                       System.out.println("Deleted file: "+ file.getName());
                   }

               }
                    try {
                        FileUtils.deleteDirectory(new File(StatisticsService.PATH_FILE));
                        System.out.println("Deleted directory: "+ StatisticsService.PATH_FILE);
                    } catch (IOException e) {
                        e.printStackTrace();
                    }
            }




    }

  /*  @Test
    public void testBirthFileExistenceAndContent(){


        StatisticsService.isFileOn = true;
        ResponseEntity<String> response = restTemplate.exchange("/statistik/birth_data/?afterDate=2000-01-01&beforeDate=2000-01-14&effectDate=2018-04-16", HttpMethod.GET, new HttpEntity<>("", new HttpHeaders()), String.class);
        if(StatisticsService.PATH_FILE != null){
            System.out.println("Path: "+ StatisticsService.PATH_FILE);
        }
        //Directory and file creation
        File folder = new File(System.getProperty("user.home") + File.separator + "statistik");
        if (folder.exists()) {

            //Checking all files in folder have content
            File[] listOfFiles = folder.listFiles();
            if (listOfFiles.length > 0) {
                for (File file : listOfFiles) {
                    if (file.isFile()) {
                        Assert.assertTrue(file.length() > 0);
                        String basename = FilenameUtils.getBaseName(file.getName());
                        String extension = FilenameUtils.getExtension(file.getName());

                        if (basename.contains(StatisticsService.ServiceName.BIRTH.name().toLowerCase())) {
                            String content;
                            try {
                                content = new String(Files.readAllBytes(Paths.get(folder + File.separator + file.getName())));
                                assertThat(extension, is("csv"));
                               /* Assert.assertEquals("\"B_Pnr\";\"B_FoedAar\";\"B_PnrGaeld\";\"B_FoedMynKod\";\"B_FoedMynKodTxt\";\"B_StatKod\";\"B_ProdDto\";\"M_Pnr\";\"M_FoedMynKod\";\"M_FoedMynKodTxt\";\"M_StatKod\";\"M_KomKod\";\"M_LokNavn\";\"M_LokKode\";\"M_VejKod\";\"M_HusNr\";\"M_Etage\";\"M_SideDoer\";\"M_Bnr\";\"F_Pnr\";\"F_FoedMynKod\";\"F_FoedMynKodTxt\";\"F_StatKod\";\"F_KomKod\";\"F_LokNavn\";\"F_LokKode\";\"F_VejKod\";\"F_HusNr\";\"F_Etage\";\"F_SideDoer\";\"F_Bnr\"\n" +
                                                "\"0101001234\";\"2000\";\"\";\"9516\";\"0\";\"5100\";\"13-01-2000\";\"2903641234\";\"6666\";\"0\";\"5100\";\"955\";\"Paamiut\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\";\"0101641234\";\"8888\";\"0\";\"5100\";\"955\";\"Paamiut\";\"PAA\";\"0001\";\"0005\";\"1\";\"tv\";\"1234\"",
                                        content.trim()
                                );
                                System.out.println(file.getName()+" file processed correctly.");
                            } catch (IOException e) {
                                e.printStackTrace();
                            }
                        } else {
                            System.out.println(file.getName()+" was not processed in this test.");
                        }
                    }
                }
            }
        } else {
            System.out.println("Folder do not exist.");
        }
    }
*/


}
