package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.PersonAddressData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonNameData;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.Lookup;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

@RestController
@RequestMapping("/statistik/address_data")
public class AddressDataService extends StatisticsService{



    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private DafoUserManager dafoUserManager;


    private Logger log = LoggerFactory.getLogger(BirthDataService.class);

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        super.get(request, response, ServiceName.ADDRESS);
    }


    @Override
    protected List<String> getColumnNames() {
       return  Arrays.asList(new String[]{
               PNR, FIRST_NAME, MIDDLE_NAME, LAST_NAME, BNR, ROAD_NAME, HOUSE_NUMBER, FLOOR_NUMBER, DOOR_NUMBER, POST_CODE, POST_DISTRICT
       });
    }

    @Override
    protected SessionManager getSessionManager() {
        return this.sessionManager;
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }

    @Override
    protected DafoUserManager getDafoUserManager() {
        return this.dafoUserManager;
    }

    @Override
    protected Logger getLogger() {
        return this.log;
    }



  /*  @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        return null;
    }*/

   // protected Map<String, String> formatSinglePerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
   @Override
   protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {

       HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());

        for (PersonRegistration registration : person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData baseData : effect.getDataItems()) {
                    PersonAddressData addressData = baseData.getAddress();
                    if (addressData != null) {
                        Lookup lookup = lookupService.doLookup(addressData.getMunicipalityCode(), addressData.getRoadCode(), addressData.getHouseNumber());
                        item.put(ROAD_NAME, lookup.roadName);
                        item.put(HOUSE_NUMBER, addressData.getHouseNumber());
                        item.put(FLOOR_NUMBER, addressData.getFloor());
                        item.put(DOOR_NUMBER, addressData.getDoor());
                        item.put(POST_CODE, Integer.toString(lookup.postalCode));
                        item.put(POST_DISTRICT, lookup.postalDistrict);
                        item.put(BNR, addressData.getBuildingNumber());

                        if (lookup.postalCode == 0) {
                            System.out.println("Failed to lookup postalcode on "+addressData.getMunicipalityCode()+"|"+addressData.getRoadCode()+" ("+lookup.roadName+")");
                        }
                    }

                    PersonNameData nameData = baseData.getName();
                    if (nameData != null) {
                        if (nameData.getFirstNames() != null && !nameData.getFirstNames().isEmpty()) {
                            item.put(FIRST_NAME, nameData.getFirstNames());
                        }
                        if (nameData.getMiddleName() != null && !nameData.getMiddleName().isEmpty()) {
                            item.put(MIDDLE_NAME, nameData.getMiddleName());
                        }
                        if (nameData.getLastName() != null && !nameData.getLastName().isEmpty()) {
                            item.put(LAST_NAME, nameData.getLastName());
                        }
                    }
                }
            }
        }
        //return Collections.singletonList(item);
        return item;
    }


    /*public void run() throws IOException {
        String inFile = "/home/lars/tmp/foo.txt";
        ArrayList<String> pnrs = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(inFile))) {
            stream.forEach(pnrs::add);
        }
        System.out.println(pnrs.size() + " pnrs loaded");

        int count = 0;
        ArrayList<PersonQuery> queries = new ArrayList<>();
        PersonQuery personQuery = new PersonQuery();
        for (String pnr : pnrs) {
            count++;
            personQuery.addPersonnummer(pnr);
            if (count >= 1000) {
                queries.add(personQuery);
                personQuery = new PersonQuery();
                count = 0;
            }
        }
        if (count > 0) {
            queries.add(personQuery);
        }


        Session session = sessionManager.getSessionFactory().openSession();
        Session lookupSession = sessionManager.getSessionFactory().openSession();
        LookupService lookupService = new LookupService(lookupSession);

        session.setDefaultReadOnly(true);
        OffsetDateTime time = OffsetDateTime.now();
        ArrayList<Map<String, String>> items = new ArrayList<>();
        Filter filter = new Filter();
        filter.effectAt = OffsetDateTime.now();
        try {
            for (PersonQuery query : queries) {
                query.setPageSize(1000);
                List<PersonEntity> personEntities = QueryManager.getAllEntities(session, query, PersonEntity.class);

                for (PersonEntity personEntity : personEntities) {
                    items.add((Map<String, String>) this.formatPerson(personEntity, lookupSession, lookupService, filter));
                }
            }
        } finally {
            session.close();
        }

        CsvSchema.Builder builder = new CsvSchema.Builder();
        builder.setColumnSeparator(';');

        CsvMapper mapper = this.csvMapper;
        mapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        mapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, true);
        mapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_EMPTY_STRINGS, true);


        List<String> keys = Arrays.asList(new String[]{
                PNR, FIRST_NAME, MIDDLE_NAME, LAST_NAME, BNR, ROAD_NAME, HOUSE_NUMBER, FLOOR_NUMBER, DOOR_NUMBER, POST_CODE, POST_DISTRICT
        });
        for (int i = 0; i < keys.size(); i++) {
            builder.addColumn(new CsvSchema.Column(
                    i, keys.get(i),
                    CsvSchema.ColumnType.STRING
            ));
        }
        CsvSchema schema = builder.build().withHeader();

        ObjectWriter writerobj = mapper.writer(schema);
        SequenceWriter writer = null;

        String outputDescription = "";

        LocalDateTime now = LocalDateTime.now();
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
        String formatDateTime = now.format(formatter);

        if (PATH_FILE != null) {
            System.out.println(PATH_FILE);
            File file = new File(PATH_FILE, "foo" + "_" + formatDateTime.toString() + ".csv");
            file.createNewFile();
            writer = writerobj.writeValues(file);
            outputDescription = "Written to file " + file.getCanonicalPath();
        }

        for (Map<String, String> item : items) {
            if (item != null) {
                writer.write(item);
            }
        }
        writer.close();

        System.out.println(outputDescription);
    }*/


}
