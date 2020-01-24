package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.CprBitemporalRecord;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.geo.GeoLookupDTO;
import dk.magenta.datafordeler.geo.GeoLookupService;
import dk.magenta.datafordeler.statistik.queries.PersonCivilStatusQuery;
import dk.magenta.datafordeler.statistik.utils.CivilStatusFilter;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toSet;

/**
 * Make a report of changes in civil-state of persons in Greenland
 */
@RestController
@RequestMapping("/statistik/civilstate_data")
public class CivilStatusDataService extends PersonStatisticsService {
    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    DafoUserManager dafoUserManager;

    @Autowired
    private CprPlugin cprPlugin;

    private String civilStatus;

    private Logger log = LogManager.getLogger(CivilStatusDataService.class.getCanonicalName());

    /**
     * Calls handlerequest in super with the ID of the report as a parameter
     * @param request
     * @param response
     * @throws AccessDeniedException
     * @throws AccessRequiredException
     * @throws InvalidTokenException
     * @throws IOException
     * @throws MissingParameterException
     * @throws InvalidClientInputException
     * @throws HttpNotFoundException
     * @throws InvalidCertificateException
     */
    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.CIVILSTATUS);
    }

    /**
     * Post is used for starting the generation of a report
     * @param request
     * @param response
     * @throws AccessDeniedException
     * @throws AccessRequiredException
     * @throws InvalidTokenException
     * @throws IOException
     * @throws MissingParameterException
     * @throws InvalidClientInputException
     * @throws HttpNotFoundException
     * @throws InvalidCertificateException
     */
    @RequestMapping(method = RequestMethod.POST, path = "/")
    public void handlePost(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.CIVILSTATUS);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                CIVIL_STATUS, CIVIL_STATUS_DATE, CITIZENSHIP_CODE, PROD_DATE, PNR, SPOUSE_PNR, AUTHORITY_CODE_TEXT, MUNICIPALITY_CODE, BIRTH_AUTHORITY, BIRTH_AUTHORITY_TEXT, BIRTH_AUTHORITY_CODE_TEXT,
                LOCALITY_NAME, LOCALITY_ABBREVIATION, LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, FLOOR_NUMBER, DOOR_NUMBER, BNR

        });
    }

    @Override
    protected Filter getFilter(HttpServletRequest request) throws Exception {
        return new CivilStatusFilter(request, this.timeintervallimit);
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

    protected String[] requiredParameters() {
        return new String[]{"registrationAfter"};
    }

    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @Override
    protected PersonCivilStatusQuery getQuery(Filter filter) {
        return new PersonCivilStatusQuery((CivilStatusFilter) filter);
    }


    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {
        List<Map<String, String>> itemMap = this.formatPersonByRecord(person, session, lookupService, (CivilStatusFilter) filter);
        if (itemMap == null || itemMap.isEmpty()) {
            return Collections.emptyList();
        }
        return itemMap;
    }

    protected List<Map<String, String>> formatPersonByRecord(PersonEntity person, Session session, GeoLookupService lookupService, CivilStatusFilter filter) {

        List<Map<String, String>> itemMap = new ArrayList<Map<String, String>>();

        OffsetDateTime searchTime = filter.registrationAfter;
        OffsetDateTime mariageEffectTime = null;

        String birthAuthorityId;
        String birthAuthorityText;
        String birthAuthorityCode;

        //Just get the first registration
        BirthPlaceDataRecord birthPlaceDataRecord = person.getBirthPlace().iterator().next();
        birthAuthorityId = Integer.toString(birthPlaceDataRecord.getAuthority());
        birthAuthorityText = birthPlaceDataRecord.getBirthPlaceName();
        birthAuthorityCode = Integer.toString(birthPlaceDataRecord.getBirthPlaceCode());

        Set<CivilStatusDataRecord> civilStatusCollection = null;
        if (filter.getCivilStatus() != null || searchTime != null) {
            civilStatusCollection = person.getCivilstatus().stream().filter(r -> (filter.getCivilStatus()== null || filter.getCivilStatus().equals(r.getCivilStatus())) &&
                    !r.isHistoric() && r.getBitemporality().registrationFrom!=null && r.getBitemporality().registrationFrom.isAfter(searchTime)
            ).collect(toSet());
        } else {
            civilStatusCollection = person.getCivilstatus();
        }

        List<PersonEventDataRecord> eventListCivilState;
        if(filter.getEventName()==null) {
            //Make a list of all civil-state-changes
            eventListCivilState = person.getEvent().stream().filter(event -> "A19".equals(event.getEventId()) ||
                    "A20".equals(event.getEventId()) || "A21".equals(event.getEventId()) ||
                    "A23".equals(event.getEventId())).collect(Collectors.toList());
        } else {
            eventListCivilState = person.getEvent().stream().filter(event -> filter.getEventName().equals(event.getEventId())).collect(Collectors.toList());
        }




        // A19 - vielse
        // A20 - skilsmisse
        // A21 doed
        // A23 reg part
        //Filter based on events
        List<CivilStatusDataRecord> filteredList = civilStatusCollection.stream().filter(empl -> eventListCivilState.stream().anyMatch(dept -> empl.getRegistrationFrom().equals(dept.getTimestamp()))).collect(Collectors.toList());

        for (CivilStatusDataRecord civilStatusDataRecord : sortRecords(FilterOnRegistrationFrom(filteredList,filter.registrationAfter,filter.registrationBefore))) {
            mariageEffectTime = civilStatusDataRecord.getEffectFrom();

            // Undone entries don't count
            if (civilStatusDataRecord.isUndone()) {
                continue;
            }

            //If this addressregistration has a sameas, it mean that is is just a close and reopen based on new timeintervals
            if (civilStatusDataRecord.getSameAs() != null) {
                continue;
            }

            //If this addressregistration has a sameas, it mean that is is just a close and reopen based on new timeintervals
            if (civilStatusDataRecord.getCorrectionof() != null) {
                continue;
            }

            if (mariageEffectTime != null && Objects.equals(mariageEffectTime, civilStatusDataRecord.getEffectTo())) {
                continue;
            }

            OffsetDateTime regFrom = civilStatusDataRecord.getRegistrationFrom();
            CprBitemporalRecord c = civilStatusDataRecord.getCorrectionof();
            if (c != null) {
                regFrom = c.getRegistrationFrom();
            }
            final OffsetDateTime firstRegFrom = regFrom;

            HashMap<String, String> item = new HashMap<>();
            item.put(PNR, person.getPersonnummer());

            item.put(CIVIL_STATUS, civilStatusDataRecord.getCivilStatus());


            item.put(SPOUSE_PNR, civilStatusDataRecord.getSpouseCpr());
            item.put(AUTHORITY_CODE_TEXT, Integer.toString(civilStatusDataRecord.getAuthority()));

            item.put(BIRTH_AUTHORITY, birthAuthorityId);
            item.put(BIRTH_AUTHORITY_TEXT, birthAuthorityText);
            item.put(BIRTH_AUTHORITY_CODE_TEXT, birthAuthorityCode);

            if (mariageEffectTime != null) {
                item.put(CIVIL_STATUS_DATE, formatTime(mariageEffectTime));
            }

            if (civilStatusDataRecord.getRegistrationFrom() != null) {
                item.put(PROD_DATE, formatTime(firstRegFrom.atZoneSameInstant(cprDataOffset)));
            }
            if (civilStatusDataRecord.getOriginDate() != null) {
                item.put(FILE_DATE, formatTime(civilStatusDataRecord.getOriginDate()));
            }

            AddressDataRecord addressDataRecord = findNewestAfterFilterOnEffect(person.getAddress(), mariageEffectTime);
            int municipalityCode = 0;
            if (addressDataRecord != null) {
                municipalityCode = addressDataRecord.getMunicipalityCode();

                item.put(MUNICIPALITY_CODE, Integer.toString(municipalityCode));
                item.put(ROAD_CODE, formatRoadCode(addressDataRecord.getRoadCode()));
                item.put(HOUSE_NUMBER, formatHouseNnr(addressDataRecord.getHouseNumber()));
                item.put(FLOOR_NUMBER, addressDataRecord.getFloor());
                item.put(DOOR_NUMBER, addressDataRecord.getDoor());
                item.put(BNR, formatBnr(addressDataRecord.getBuildingNumber()));
                GeoLookupDTO lookup = lookupService.doLookup(
                        addressDataRecord.getMunicipalityCode(),
                        addressDataRecord.getRoadCode(),
                        addressDataRecord.getHouseNumber()
                );
                if (lookup != null) {
                    if (lookup.getLocalityName() != null) {
                        item.put(LOCALITY_NAME, lookup.getLocalityName());
                    }
                    if (lookup.getLocalityAbbrev() != null) {
                        item.put(LOCALITY_ABBREVIATION, lookup.getLocalityAbbrev());
                    }
                    if (lookup.getLocalityCode() != null) {
                        item.put(LOCALITY_CODE, lookup.getLocalityCode());
                    }
                }
            }

            CitizenshipDataRecord citizenshipDataRecord = findNewestAfterFilterOnEffect(person.getCitizenship(), mariageEffectTime);
            if (citizenshipDataRecord != null) {
                item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
            }
            if(municipalityCode > 950) {
                replaceMapValues(item, null, "");
                itemMap.add(item);
            }
        }
        return itemMap;
    }
}