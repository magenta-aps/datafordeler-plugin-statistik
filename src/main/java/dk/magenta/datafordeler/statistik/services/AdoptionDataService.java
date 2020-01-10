package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
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
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Collectors;


@RestController
@RequestMapping("/statistik/adoption_data")
public class AdoptionDataService extends PersonStatisticsService {

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
                PNR, BIRTHDAY_YEAR, MOTHER_PREFIX + PNR, FATHER_PREFIX + PNR, "AM_Foer_Pnr", "AF_Foer_Pnr",
        BIRTH_AUTHORITY, CITIZENSHIP_CODE, PROD_DATE, FILE_DATE, "AdoptionDto", MUNICIPALITY_CODE, LOCALITY_NAME, LOCALITY_ABBREVIATION,
                LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, FLOOR_NUMBER, FLOOR_NUMBER, BNR
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

    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {

        List<Map<String, String>> ll = new ArrayList<Map<String, String>>();

        HashMap<String, String> itema = new HashMap<>(this.formatPersonByRecord(false, person, session, lookupService, filter));
        if (itema.isEmpty()) {
            return Collections.emptyList();
        }
        itema.put(PRE + PNR, person.getPersonnummer());
        ll.add(itema);

        HashMap<String, String> itemb = new HashMap<>(this.formatPersonByRecord(true, person, session, lookupService, filter));
        if (itemb.isEmpty()) {
            return Collections.emptyList();
        }
        itemb.put(POST + PNR, person.getPersonnummer());
        ll.add(itemb);

        return ll;
    }


    protected Map<String, String> formatPersonByRecord(boolean before, PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {

        // Map of effectTime to addresses (when address was moved into)
        HashMap<OffsetDateTime, CprBitemporalRecord> addresses = new HashMap<>();
        // Map of effectTime to registrationTime (when this move was first registered)

        //Make a list of all adoption-events
        List<PersonEventDataRecord> eventListAdoption = person.getEvent().stream().filter(event -> "A35".equals(event.getEventId())).collect(Collectors.toList());

        HashMap<String, String> item = new HashMap<>();

        Set<ParentDataRecord> fatherList = person.getFather();
        Set<ParentDataRecord> motherList = person.getMother();

        if((fatherList.size()<=1 && motherList.size()<=1) || eventListAdoption.size()==0) {
            return item;
        }

        item.put(PNR, person.getPersonnummer());

        System.out.println(person.getPersonnummer());

        OffsetDateTime eventtimestamp = eventListAdoption.get(0).getTimestamp();
        System.out.println(eventtimestamp);

        if(before) {
            item.put("CODE", "PRE");
            ParentDataRecord prefather = findRegistrationAtMatchingChangedtimePre(fatherList, eventtimestamp);
            ParentDataRecord premother = findRegistrationAtMatchingChangedtimePre(motherList, eventtimestamp);
            if(prefather!=null) {
                item.put(FATHER_PREFIX + PNR, prefather.getCprNumber());
                item.put(FILE_DATE, formatTime(prefather.getOriginDate()));
            }
            if(premother!=null) {
                item.put(MOTHER_PREFIX + PNR, premother.getCprNumber());
                item.put(FILE_DATE, formatTime(premother.getOriginDate()));
            }
            item.put("KomKod", ""+findRegistrationAtMatchingChangedtimePre(person.getAddress(), eventtimestamp).getMunicipalityCode());


            //item.put("AM_Foer_Pnr", prefather.getCprNumber());


        } else {
            item.put("CODE", "POST");
            ParentDataRecord postfather = findRegistrationAtMatchingChangedtimePost(fatherList, eventtimestamp);
            ParentDataRecord postmother = findRegistrationAtMatchingChangedtimePost(motherList, eventtimestamp);
            if(postfather!=null) {
                item.put(FATHER_PREFIX + PNR, postfather.getCprNumber());
                item.put(FILE_DATE, formatTime(postfather.getOriginDate()));
            }
            if(postmother!=null) {
                item.put(MOTHER_PREFIX + PNR, postmother.getCprNumber());
                item.put(FILE_DATE, formatTime(postmother.getOriginDate()));
            }
            item.put("KomKod", ""+findRegistrationAtMatchingChangedtimePre(person.getAddress(), eventtimestamp).getMunicipalityCode());
        }

        item.put(BIRTHDAY_YEAR, Integer.toString(findNewestUnclosed(person.getBirthTime()).getBirthDatetime().getYear()));
        item.put(BIRTHDAY_YEAR, Integer.toString(findNewestUnclosed(person.getBirthTime()).getBirthDatetime().getYear()));
        item.put(PROD_DATE, formatTime(eventtimestamp));
        item.put("AdoptionDto", formatTime(eventtimestamp));


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


        //replaceMapValues(item, null, "");
        return item;
    }


    private static <R extends CprBitemporalRecord> R filter(Collection<R> records, Filter filter) {
        return findMostImportant(filterRecordsByEffect(filterUndoneRecords(records), filter.effectAt));
    }
}
