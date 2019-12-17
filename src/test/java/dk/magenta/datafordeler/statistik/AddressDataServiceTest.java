package dk.magenta.datafordeler.statistik;


import com.fasterxml.jackson.core.JsonProcessingException;
import dk.magenta.datafordeler.core.Application;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.util.InputStreamReader;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.services.AddressDataService;
import org.apache.commons.io.FilenameUtils;
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

import javax.crypto.Cipher;
import javax.crypto.KeyGenerator;
import javax.crypto.SecretKey;
import java.io.File;
import java.util.ArrayList;
import java.util.zip.ZipFile;

@RunWith(SpringJUnit4ClassRunner.class)
@ContextConfiguration(classes = Application.class)
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
public class AddressDataServiceTest extends TestBase {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private TestRestTemplate restTemplate;

    @Autowired
    private TestUtils testsUtils;

    @Autowired
    private AddressDataService addressDataService;

    @Autowired
    private TestUtil testUtil;

    TestUserDetails testUserDetails;
    @Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadPersonData("bornperson.txt");
        this.loadAllGeoAdress(sessionManager);
        addressDataService.setUseTimeintervallimit(false);
    }

    @Test
    public void testService() throws JsonProcessingException {
        addressDataService.setWriteToLocalFile(false);

        testUserDetails = new TestUserDetails();
        testUserDetails.giveAccess(CprRolesDefinition.READ_CPR_ROLE);
        testsUtils.applyAccess(testUserDetails);

        ResponseEntity<String> response = restTemplate.exchange("/statistik/address_data/?showfrontpage=true&registrationAfter=2000-01-01", HttpMethod.GET, new HttpEntity("", new HttpHeaders()), String.class);
        Assert.assertEquals(InputStreamReader.readInputStream(AddressDataService.class.getResourceAsStream("/addressServiceForm.html")), response.getBody());

        MultiValueMap<String,Object> form = new LinkedMultiValueMap<String,Object>();
        form.add("file", new InputStreamResource(AddressDataServiceTest.class.getResourceAsStream("/addressInput.csv")));

        response = restTemplate.exchange("/statistik/address_data/?registrationAfter=2000-01-01", HttpMethod.POST, new HttpEntity(form, new HttpHeaders()), String.class);
        String expected = "\"Pnr\";\"Fornavn\";\"Mellemnavn\";\"Efternavn\";\"Bnr\";\"VejNavn\";\"HusNr\";\"Etage\";\"SideDoer\";\"Postnr\";\"PostDistrikt\"\n" +
                "\"0101001234\";\"Tester Testmember\";;\"Testersen\";\"1234\";\"Qarsaalik\";\"18\";\"1\";\"tv\";\"3900\";\"Nuuk\";\n";
        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );

    }

    @Test
    public void testZip()  {

        try{
            KeyGenerator keygenerator = KeyGenerator.getInstance("DES");
            SecretKey myDesKey = keygenerator.generateKey();

            Cipher desCipher;
            desCipher = Cipher.getInstance("DES");

            byte[] text = "No body can see me.".getBytes("UTF8");

            desCipher.init(Cipher.ENCRYPT_MODE, myDesKey);
            byte[] textEncrypted = desCipher.doFinal(text);

            String s = new String(textEncrypted);
            System.out.println(s);

            desCipher.init(Cipher.DECRYPT_MODE, myDesKey);
            byte[] textDecrypted = desCipher.doFinal(textEncrypted);

            s = new String(textDecrypted);
            System.out.println(s);
        }catch(Exception e)
        {
            System.out.println("Exception");
        }

    }


}