package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
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
import org.apache.commons.io.IOUtils;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.time.OffsetDateTime;
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

    @Autowired
    private CprPlugin cprPlugin;


    private Logger log = LoggerFactory.getLogger(BirthDataService.class);

    @Override
    protected DafoUserDetails getUser(HttpServletRequest request) throws InvalidTokenException {
        String formToken = request.getParameter("token");
        if (formToken != null) {
            return this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);
        }
        return super.getUser(request);
    }

    @RequestMapping(method = RequestMethod.POST, path = "/")
    public void handlePost(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        super.handleRequest(request, response, ServiceName.ADDRESS);
    }

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void handleGet(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        IOUtils.copy(
                AddressDataService.class.getResourceAsStream("/addressServiceForm.html"),
                response.getWriter()
        );
    }


    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
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

    @Override
    protected List<PersonQuery> getQueryList(HttpServletRequest request) throws IOException {

        ArrayList<String> pnrs = new ArrayList<>();
        InputStream testInput = AddressDataService.class.getResourceAsStream("/addressInput.csv");
        Stream<String> stream = new BufferedReader(new InputStreamReader(testInput)).lines();

        // TODO: Replace the above with obtaining input from an uploaded file in the POST request
        // This means obtaining a Part from the request object (looping through request.getParts()),
        // finding the correct one (likely based on name), and reading the part.getInputStream() into our stream object

        try {
            stream.forEach(pnrs::add);
        } finally {
            stream.close();
            testInput.close();
        }
        System.out.println(pnrs.size() + " pnrs loaded");

        int count = 0;
        ArrayList<PersonQuery> queries = new ArrayList<>();
        PersonQuery personQuery = new PersonQuery();
        for (String pnr : pnrs) {
            count++;
            personQuery.addPersonnummer(pnr);
            // The database complains when there's more that 2100 values in a comparison list,
            // so split the query into chucks of a reasonable size. 1000 is chosen.
            if (count >= 1000) {
                queries.add(personQuery);
                personQuery = new PersonQuery();
                count = 0;
            }
        }
        if (count > 0) {
            queries.add(personQuery);
        }

        return queries;
    }




    //---

   @Override
   protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {

       HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());
        OffsetDateTime effectTime = filter.effectAt;

        for (PersonRegistration registration : person.getRegistrations()) {
            for (PersonEffect effect : effectTime != null ? registration.getEffectsAt(effectTime) : registration.getEffects()) {
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
        return Collections.singletonList(item);
    }

    @Override
    protected Filter getFilter(HttpServletRequest request) {
        Filter filter = super.getFilter(request);
        filter.effectAt = OffsetDateTime.now();
        return filter;
    }


}
