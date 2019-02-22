package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.cpr.records.person.data.AddressDataRecord;
import dk.magenta.datafordeler.cpr.records.person.data.NameDataRecord;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.Lookup;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.apache.commons.io.IOUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@RestController
@RequestMapping("/statistik/address_data")
public class AddressDataService extends PersonStatisticsService {


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

    @PostConstruct
    public void init() {
        this.setWriteToLocalFile(false);
    }


    private Logger log = LogManager.getLogger(BirthDataService.class.getCanonicalName());

    @Override
    protected DafoUserDetails getUser(HttpServletRequest request) throws InvalidTokenException, AccessDeniedException, InvalidCertificateException {
        String formToken = request.getParameter("token");
        if (formToken != null) {
            return this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);
        }
        return super.getUser(request);
    }

    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
        } catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw (e);
        }
    }

    @RequestMapping(method = RequestMethod.POST, path = "/")
    public void handlePost(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {
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
        return Arrays.asList(new String[]{
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

    private static Pattern numeric = Pattern.compile("\\d+");
    private static int limit = 1000;

    @Override
    protected List<PersonRecordQuery> getQueryList(Filter filter) throws IOException {

        ArrayList<PersonRecordQuery> queries = new ArrayList<>();

        if (filter.onlyPnr != null) {
            List<String> pnrs = filter.onlyPnr;

            System.out.println("Got " + pnrs.size() + " lines");

            int count = 0;
            PersonRecordQuery personQuery = new PersonRecordQuery();
            personQuery.setPageSize(limit);
            for (String pnr : pnrs) {
                if (count == 0 && !numeric.matcher(pnr).matches()) {
                    continue;
                }
                count++;
                personQuery.addPersonnummer(pnr);
                // The database complains when there's more that 2100 values in a comparison list,
                // so split the query into chunks of a reasonable size. <limit> is chosen.
                if (count >= limit) {
                    queries.add(personQuery);
                    personQuery = new PersonRecordQuery();
                    personQuery.setPageSize(limit);
                    count = 0;
                }
            }
            if (count > 0) {
                queries.add(personQuery);
            }
        }
        return queries;
    }

    //---

    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {

        HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());
        OffsetDateTime effectTime = filter.effectAt;

        List<AddressDataRecord> records = sortRecords(person.getAddress());
        for (AddressDataRecord addressData : records) {
            if (addressData.getBitemporality().registrationTo == null && addressData.getBitemporality().containsEffect(effectTime, effectTime)) {
                Lookup lookup = lookupService.doLookup(addressData.getMunicipalityCode(), addressData.getRoadCode(), addressData.getHouseNumber());
                item.put(ROAD_NAME, lookup.roadName);
                item.put(HOUSE_NUMBER, addressData.getHouseNumber());
                item.put(FLOOR_NUMBER, addressData.getFloor());
                item.put(DOOR_NUMBER, addressData.getDoor());
                item.put(POST_CODE, Integer.toString(lookup.postalCode));
                item.put(POST_DISTRICT, lookup.postalDistrict);
                String bnr = addressData.getBuildingNumber();
                if (bnr == null || bnr.isEmpty()) {
                    bnr = lookup.bNumber;
                }
                item.put(BNR, bnr);

                if (lookup.postalCode == 0) {
                    System.out.println("Failed to lookup postalcode on " + addressData.getMunicipalityCode() + "|" + addressData.getRoadCode() + " (" + lookup.roadName + ")");
                }
            }
        }
        Set<NameDataRecord> nameDataRecords = person.getName();
        for (NameDataRecord nameDataRecord : nameDataRecords) {
            if (nameDataRecord.getBitemporality().registrationTo == null && nameDataRecord.getBitemporality().containsEffect(effectTime, effectTime)) {
                if (nameDataRecord.getFirstNames() != null && !nameDataRecord.getFirstNames().isEmpty()) {
                    item.put(FIRST_NAME, nameDataRecord.getFirstNames());
                }
                if (nameDataRecord.getMiddleName() != null && !nameDataRecord.getMiddleName().isEmpty()) {
                    item.put(MIDDLE_NAME, nameDataRecord.getMiddleName());
                }
                if (nameDataRecord.getLastName() != null && !nameDataRecord.getLastName().isEmpty()) {
                    item.put(LAST_NAME, nameDataRecord.getLastName());
                }
            }
        }
        return Collections.singletonList(item);
    }

    @Override
    protected Filter getFilter(HttpServletRequest request) {
        Filter filter = new Filter();

        InputStream inputStream = null;
        String fieldName = "file"; // Matches the file field in addressServiceForm.html
        try {
            Part filePart = request.getPart(fieldName);
            if (filePart != null) {
                inputStream = filePart.getInputStream();
            }
        } catch (ServletException | IOException e) {
            e.printStackTrace();
        }
        if (inputStream != null) {

            ArrayList<String> pnrs = new ArrayList<>();
            Stream<String> stream = new BufferedReader(new InputStreamReader(inputStream)).lines();

            try {
                stream.forEach(pnrs::add);
            } finally {
                stream.close();
                try {
                    inputStream.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            filter.onlyPnr = pnrs;
        }

        filter.effectAt = OffsetDateTime.now();
        return filter;
    }


}
