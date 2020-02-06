package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.cpr.records.CprBitemporalRecord;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.geo.GeoLookupDTO;
import dk.magenta.datafordeler.geo.GeoLookupService;
import dk.magenta.datafordeler.statistik.queries.PersonAdoptionQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/statistik/adoption_data")
public class AdoptionDataService extends PersonStatisticsService {

    private class Exclude extends Exception {
    }

    private static final String PRE = "PRE";
    private static final String POST = "POST";


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

    private Logger log = LogManager.getLogger(AdoptionDataService.class);

    @PostConstruct
    public void setUseTimeintervallimit() {
        super.setUseTimeintervallimit(false);
    }

    /**
     * Calls handleRequest in super with the ID of the report as a parameter
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
        super.handleRequest(request, response, ServiceName.ADOPTION);
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
        super.handleRequest(request, response, ServiceName.ADOPTION);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{"CODE",
                PNR, BIRTHDAY_YEAR, MOTHER_PREFIX + PNR, FATHER_PREFIX + PNR, AM_mynkod, AF_mynkod,
                BIRTH_AUTHORITY, CITIZENSHIP_CODE, PROD_DATE, FILE_DATE, ADOPTIONDTO, MUNICIPALITY_CODE, LOCALITY_NAME, LOCALITY_ABBREVIATION,
                LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, FLOOR_NUMBER, BNR,
                MOTHER_PREFIX + MUNICIPALITY_CODE, MOTHER_PREFIX + ROAD_CODE, MOTHER_PREFIX + HOUSE_NUMBER,
                MOTHER_PREFIX + FLOOR_NUMBER, MOTHER_PREFIX + DOOR_NUMBER, MOTHER_PREFIX + BNR, MOTHER_PREFIX + LOCALITY_NAME,
                MOTHER_PREFIX + LOCALITY_ABBREVIATION, MOTHER_PREFIX + LOCALITY_CODE, MOTHER_PREFIX + CITIZENSHIP_CODE,
                MOTHER_PREFIX + BIRTH_AUTHORITY, MOTHER_PREFIX + BIRTH_AUTHORITY_TEXT,
                FATHER_PREFIX + MUNICIPALITY_CODE, FATHER_PREFIX + ROAD_CODE, FATHER_PREFIX + HOUSE_NUMBER,
                FATHER_PREFIX + FLOOR_NUMBER, FATHER_PREFIX + DOOR_NUMBER, FATHER_PREFIX + BNR, FATHER_PREFIX + LOCALITY_NAME,
                FATHER_PREFIX + LOCALITY_ABBREVIATION, FATHER_PREFIX + LOCALITY_CODE, FATHER_PREFIX + CITIZENSHIP_CODE,
                FATHER_PREFIX + BIRTH_AUTHORITY, FATHER_PREFIX + BIRTH_AUTHORITY_TEXT
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

    protected String[] requiredParameters() {
        return new String[]{"registrationAfter"};
    }

    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @Override
    protected PersonRecordQuery getQuery(Filter filter) {
        return new PersonAdoptionQuery(filter);
    }

    /**
     * Create formatted adoptioninfo for one adopted child
     * @param person
     * @param session
     * @param lookupService
     * @param filter
     * @return
     */
    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {

        List<Map<String, String>> fullAdoptionResult = new ArrayList<Map<String, String>>();

        HashMap<String, String> postAdoptionLine = new HashMap<>(this.formatPersonByRecord(true, person, session, lookupService, filter));
        if (postAdoptionLine.isEmpty()) {
            return Collections.emptyList();
        }
        postAdoptionLine.put(POST + PNR, person.getPersonnummer());
        fullAdoptionResult.add(postAdoptionLine);

        HashMap<String, String> preAdoptionLine = new HashMap<>(this.formatPersonByRecord(false, person, session, lookupService, filter));
        if (preAdoptionLine.isEmpty()) {
            return Collections.emptyList();
        }
        preAdoptionLine.put(PRE + PNR, person.getPersonnummer());
        fullAdoptionResult.add(preAdoptionLine);

        return fullAdoptionResult;
    }

    /**
     * Create one formatted row of data for one adopted child
     * @param before
     * @param person
     * @param session
     * @param lookupService
     * @param filter
     * @return
     */
    protected Map<String, String> formatPersonByRecord(boolean before, PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {

        // Map of effectTime to addresses (when address was moved into)
        HashMap<OffsetDateTime, CprBitemporalRecord> addresses = new HashMap<>();

        //Make a list of all adoption-events
        List<PersonEventDataRecord> eventListAdoption = person.getEvent().stream().filter(event -> "A35".equals(event.getEventId())).collect(Collectors.toList());

        HashMap<String, String> item = new HashMap<>();

        Set<ParentDataRecord> fatherList = person.getFather();
        Set<ParentDataRecord> motherList = person.getMother();

        if((fatherList.size()<=1 && motherList.size()<=1) || eventListAdoption.size()==0) {
            return item;
        }

        item.put(PNR, person.getPersonnummer());
        OffsetDateTime eventtimestamp = eventListAdoption.get(0).getTimestamp();

        if(before) {
            item.put("CODE", PRE);
            ParentDataRecord prefather = findRegistrationAtMatchingChangedtimePre(fatherList, eventtimestamp);
            ParentDataRecord premother = findRegistrationAtMatchingChangedtimePre(motherList, eventtimestamp);
            if(prefather!=null) {
                item.put(FATHER_PREFIX + PNR, prefather.getCprNumber());
                item.put(AF_mynkod, Integer.toString(prefather.getAuthority()));
                PersonEntity parent = QueryManager.getEntity(session, PersonEntity.generateUUID(prefather.getCprNumber()), PersonEntity.class);
                if (parent != null) {
                    item.putAll(this.formatParentPersonByRecord(parent, FATHER_PREFIX, lookupService, eventtimestamp, true));
                }
            }
            if(premother!=null) {
                item.put(MOTHER_PREFIX + PNR, premother.getCprNumber());
                item.put(AM_mynkod, Integer.toString(premother.getAuthority()));
                PersonEntity parent = QueryManager.getEntity(session, PersonEntity.generateUUID(premother.getCprNumber()), PersonEntity.class);
                if (parent != null) {
                    item.putAll(this.formatParentPersonByRecord(parent, MOTHER_PREFIX, lookupService, eventtimestamp, true));
                }
            }
        } else {
            item.put("CODE", POST);
            ParentDataRecord postfather = findRegistrationAtMatchingChangedtimePost(fatherList, eventtimestamp);
            ParentDataRecord postmother = findRegistrationAtMatchingChangedtimePost(motherList, eventtimestamp);
            if(postfather!=null) {
                item.put(FATHER_PREFIX + PNR, postfather.getCprNumber());
                item.put(AF_mynkod, Integer.toString(postfather.getAuthority()));
                item.put(FILE_DATE, formatTime(postfather.getOriginDate()));//This is the same for mother and father record
                PersonEntity parent = QueryManager.getEntity(session, PersonEntity.generateUUID(postfather.getCprNumber()), PersonEntity.class);
                if (parent != null) {
                    item.putAll(this.formatParentPersonByRecord(parent, FATHER_PREFIX, lookupService, eventtimestamp, true));
                }
            }
            if(postmother!=null) {
                item.put(MOTHER_PREFIX + PNR, postmother.getCprNumber());
                item.put(AM_mynkod, Integer.toString(postmother.getAuthority()));
                item.put(FILE_DATE, formatTime(postmother.getOriginDate()));//This is the same for mother and father record
                PersonEntity parent = QueryManager.getEntity(session, PersonEntity.generateUUID(postmother.getCprNumber()), PersonEntity.class);
                if (parent != null) {
                    item.putAll(this.formatParentPersonByRecord(parent, MOTHER_PREFIX, lookupService, eventtimestamp, true));
                }
            }

        }


        LocalDateTime birthDateTime = findNewestUnclosed(person.getBirthTime()).getBirthDatetime();
        item.put(BIRTHDAY_YEAR, Integer.toString(birthDateTime.getYear()));
        item.put(PROD_DATE, formatTime(eventtimestamp));
        item.put(ADOPTIONDTO, formatTime(eventtimestamp));

        CitizenshipDataRecord citizenshipDataRecord = findNewestUnclosedWithSpecifiedEffect(person.getCitizenship(), eventtimestamp);
        if (citizenshipDataRecord != null) {
            item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
        }

        //Getinformation about the adopted persons birth
        BirthPlaceDataRecord birthPlaceDataRecord = findNewestUnclosedWithSpecifiedEffect(person.getBirthPlace(), eventtimestamp);
        if (birthPlaceDataRecord != null) {
            item.put(BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
            item.put(BIRTH_AUTHORITY_TEXT, birthPlaceDataRecord.getBirthPlaceName());
            item.put(BIRTH_AUTHORITY_CODE_TEXT, Integer.toString(birthPlaceDataRecord.getBirthPlaceCode()));
        }


        AddressDataRecord addressDataRecord = findNewestAfterFilterOnEffect(person.getAddress(), eventtimestamp);
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

        replaceMapValues(item, null, "");
        return item;
    }


    private Map<String, String> formatParentPersonByRecord(PersonEntity person, String prefix, GeoLookupService lookupService, OffsetDateTime birthEffectTime, boolean excludeIfNonGreenlandic) {
        HashMap<String, String> item = new HashMap<>();

        Filter filterf = new Filter();
        filterf.effectAt = birthEffectTime;
        filterf.registrationAt = birthEffectTime;
        AddressDataRecord addressDataRecord = filter(person.getAddress(), filterf);

        if (excludeIfNonGreenlandic && addressDataRecord == null) {
            log.warn("addressDataRecord==null");
            return item;
        }

        item.put(prefix + MUNICIPALITY_CODE, Integer.toString(addressDataRecord.getMunicipalityCode()));
        if (excludeIfNonGreenlandic && addressDataRecord.getMunicipalityCode() < 955) {
            log.warn("NOT GL ADD " + addressDataRecord.getId());
            return item;
        }
        GeoLookupDTO lookup = lookupService.doLookup(
                addressDataRecord.getMunicipalityCode(),
                addressDataRecord.getRoadCode(),
                addressDataRecord.getHouseNumber()
        );
        item.put(prefix + MUNICIPALITY_CODE, Integer.toString(addressDataRecord.getMunicipalityCode()));
        item.put(prefix + ROAD_CODE, formatRoadCode(addressDataRecord.getRoadCode()));
        item.put(prefix + HOUSE_NUMBER, formatHouseNnr(addressDataRecord.getHouseNumber()));
        item.put(prefix + FLOOR_NUMBER, addressDataRecord.getFloor());
        item.put(prefix + DOOR_NUMBER, addressDataRecord.getDoor());
        item.put(prefix + BNR, formatBnr(addressDataRecord.getBuildingNumber()));

        if (lookup.getLocalityName() != null) {
            item.put(prefix + LOCALITY_NAME, lookup.getLocalityName());
        }
        if (lookup.getLocalityAbbrev() != null) {
            item.put(prefix + LOCALITY_ABBREVIATION, lookup.getLocalityAbbrev());
        }
        if (lookup.getLocalityCode() != null) {
            item.put(prefix + LOCALITY_CODE, lookup.getLocalityCode());
        }

        CitizenshipDataRecord citizenshipDataRecord = findNewestAfterFilterOnEffect(person.getCitizenship(), birthEffectTime);
        if (citizenshipDataRecord != null) {
            item.put(prefix + CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
        }

        BirthPlaceDataRecord birthPlaceDataRecord = findNewestAfterFilterOnEffect(person.getBirthPlace(), birthEffectTime);
        if (birthPlaceDataRecord != null) {
            item.put(prefix + BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
            item.put(prefix + BIRTH_AUTHORITY_TEXT, birthPlaceDataRecord.getBirthPlaceName());
        }
        return item;
    }

    private static <R extends CprBitemporalRecord> R filter(Collection<R> records, Filter filter) {
        return findMostImportant(filterRecordsByEffect(filterUndoneRecords(records), filter.effectAt));
    }

}
