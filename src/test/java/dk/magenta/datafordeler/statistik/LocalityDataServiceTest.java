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

    //WE have no testdata and this is not considered ar real unittest
    //@Before
    public void initialize() throws Exception {
        testsUtils.setPath();
        testsUtils.loadGeoLocalityData("Lokalitet_test.json");
        testsUtils.loadGeoRoadData("Vejmidte_test.json");
    }

    @Test
    public void testDummy() {
    }

    //WE have no testdata and this is not considered ar real unittest
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
        String expected = "\"KomKod\";\"KomKortNavn\";\"KomNavn\";\"LokKode\";\"LokKortNavn\";\"LokNavn\";\"LokTypeKod\";\"LokTypeNavn\";\"LokStatusKod\";\"LokStatusNavn\"\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1700\";\"QNQ\";\"Qaanaaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1706\";\"MOR\";\"Moriusaq\";\"4\";\"Nedlagt bygd\";\"20\";\"Nedlagt\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1704\";\"SIO\";\"Siorapaluk\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1701\";\"SAV\";\"Savissivik\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1601\";\"UPK\";\"Upernavik Kujalleq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1606\";\"NUS\";\"Nuussuaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1600\";\"UPV\";\"Upernavik\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1602\";\"KAQ\";\"Kangersuatsiaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1605\";\"TSS\";\"Tasiusaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1603\";\"AAP\";\"Aappilattoq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1608\";\"NAJ\";\"Naajaat\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1610\";\"NUT\";\"Nutaarmiut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1607\";\"KLQ\";\"Kullorsuaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1604\";\"TUS\";\"Tussaaq\";\"4\";\"Nedlagt bygd\";\"20\";\"Nedlagt\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1200\";\"ILU\";\"Ilulissat\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1609\";\"INN\";\"Innaarsuit\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"0600\";\"NUK\";\"Nuuk\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1403\";\"KLK\";\"Kangerluk\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1203\";\"SQQ\";\"Saqqaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"0906\";\"NQK\";\"Niaqornaarsuk\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1504\";\"SAA\";\"Saattut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0703\";\"KAM\";\"Kangaamiut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1502\";\"QST\";\"Qaarsut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1201\";\"OQA\";\"Oqaatsut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1101\";\"IKA\";\"Ikamiut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0803\";\"SFN\";\"Sarfannguit\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"0902\";\"ATT\";\"Attu\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0701\";\"ATA\";\"Atammik\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"0601\";\"QTT\";\"Qeqertarsuatsiaat\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"0605\";\"KAP\";\"Kapisillit\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1100\";\"QAS\";\"Qasigiannguit\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1503\";\"IKE\";\"Ikerasak\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"0900\";\"KAT\";\"Kangaatsiaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1500\";\"UUM\";\"Uummannaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1003\";\"AKU\";\"Akunnaaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1507\";\"NUG\";\"Nuugaatsiaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"0500\";\"PAA\";\"Paamiut\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1000\";\"AAS\";\"Aasiaat\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1400\";\"QEQ\";\"Qeqertarsuaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0801\";\"ITI\";\"Itilleq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"0501\";\"ARS\";\"Arsuk\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"0908\";\"IKS\";\"Ikerasaarsuk\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"0905\";\"IGF\";\"Iginniarfik\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0702\";\"NAP\";\"Napasoq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0820\";\"KAN\";\"Kangerlussuaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1204\";\"ILQ\";\"Ilimanaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1505\";\"UKK\";\"Ukkusissat\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1506\";\"ILL\";\"Illorsuit\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0800\";\"SIS\";\"Sisimiut\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1202\";\"QQT\";\"Qeqertaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"957\";\"QE\";\"Qeqqata\";\"0700\";\"MAN\";\"Maniitsoq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"959\";\"KQ\";\"Qeqertalik\";\"1004\";\"KIT\";\"Kitsissuarsuit\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"960\";\"AK\";\"Avannaata\";\"1501\";\"NIA\";\"Niaqornat\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0204\";\"QSM\";\"Qassimiut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0321\";\"NRS\";\"Narsarsuaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0305\";\"QSK\";\"Qassiarsuk\";\"5\";\"Fåreholdersted\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0300\";\"NAR\";\"Narsaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0104\";\"TAQ\";\"Tasiusaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0201\";\"SAL\";\"Saarloq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0108\";\"ALP\";\"Alluitsup Paa\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0200\";\"QAQ\";\"Qaqortoq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0103\";\"NKJ\";\"Narsaq Kujalleq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0302\";\"IGA\";\"Igaliku\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0102\";\"APL\";\"Aappilattoq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0202\";\"EQA\";\"Eqalugaarsuit\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0106\";\"AMS\";\"Ammassivik\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"955\";\"KU\";\"Kujalleq\";\"0100\";\"NAN\";\"Nanortalik\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1805\";\"TIN\";\"Tiilerilaaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1803\";\"ISO\";\"Isertoq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1804\";\"KUL\";\"Kulusuk\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1800\";\"TAS\";\"Tasiilaq\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1802\";\"SML\";\"Sermiligaaq\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1900\";\"ILT\";\"Ittoqqortoormiit\";\"1\";\"By\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1806\";\"KUM\";\"Kuummiut\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1902\";\"ITR\";\"Itterajivit\";\"4\";\"Nedlagt bygd\";\"20\";\"Nedlagt\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1901\";\"UUN\";\"Uunarteq\";\"4\";\"Nedlagt bygd\";\"20\";\"Nedlagt\";\n" +
                "\"956\";\"SE\";\"Sermersooq\";\"1906\";\"NRI\";\"Nerlerit Inaat\";\"3\";\"Bygd\";\"15\";\"Aktiv\";\n";

        Assert.assertEquals(
                testUtil.csvToJsonString(expected),
                testUtil.csvToJsonString(response.getBody().trim())
        );



    }
}