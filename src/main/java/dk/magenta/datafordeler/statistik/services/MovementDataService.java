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
import dk.magenta.datafordeler.cpr.records.CprNontemporalRecord;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.geo.GeoLookupDTO;
import dk.magenta.datafordeler.geo.GeoLookupService;
import dk.magenta.datafordeler.statistik.queries.PersonMoveQuery;
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

/*Created by Efrin 06-04-2018*/


@RestController
@RequestMapping("/statistik/movement_data")
public class MovementDataService extends PersonStatisticsService {

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

    private Logger log = LogManager.getLogger(MovementDataService.class.getCanonicalName());


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
        super.handleRequest(request, response, ServiceName.MOVEMENT);
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
        super.handleRequest(request, response, ServiceName.MOVEMENT);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, EFFECTIVE_PNR, STATUS_CODE, BIRTH_AUTHORITY, CITIZENSHIP_CODE,
                MOTHER_PNR, FATHER_PNR, SPOUSE_PNR, PROD_DATE, FILE_DATE, MOVE_DATE,
                ORIGIN_COUNTRY_CODE, ORIGIN_MUNICIPALITY_CODE, ORIGIN_LOCALITY_NAME, ORIGIN_ROAD_CODE, ORIGIN_HOUSE_NUMBER, ORIGIN_FLOOR, ORIGIN_DOOR_NUMBER, ORIGIN_BNR,
                DESTINATION_COUNTRY_CODE, DESTINATION_MUNICIPALITY_CODE, DESTINATION_LOCALITY_NAME, DESTINATION_ROAD_CODE, DESTINATION_HOUSE_NUMBER, DESTINATION_FLOOR, DESTINATION_DOOR_NUMBER, DESTINATION_BNR
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
        return new PersonMoveQuery(filter);
    }


    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {
        return this.formatPersonByRecord(person, session, lookupService, filter);
    }

    public List<Map<String, String>> formatPersonByRecord(PersonEntity person, Session session, GeoLookupService lookupService, Filter filter) {
        // Map of effectTime to addresses (when address was moved into)
        HashMap<OffsetDateTime, CprBitemporalRecord> addresses = new HashMap<>();
        // Map of effectTime to registrationTime (when this move was first registered)

        //Make a list of all moving-events
        List<PersonEventDataRecord> eventListMove = person.getEvent().stream().filter(event -> "A01".equals(event.getEventId()) ||
                "A05".equals(event.getEventId()) || "A06".equals(event.getEventId())).collect(Collectors.toList());

        for (AddressDataRecord addressDataRecord : sortRecords(person.getAddress())) {
            OffsetDateTime effectDate = addressDataRecord.getEffectFrom();
            // Undone entries don't count
            if (addressDataRecord.isUndone()) {
                continue;
            }
            if (isUndone(addressDataRecord)) {
                continue;
            }
            // Corrected records will be represented by their correctors
            if (addressDataRecord.getCorrectors().size() > 0) {
                continue;
            }

            //If this addressregistration has a sameas, it mean that is is just a close and reopen based on new timeintervals
            if (addressDataRecord.getSameAs() != null) {
                continue;
            }

            if (effectDate != null && Objects.equals(effectDate, addressDataRecord.getEffectTo())) {
                continue;
            }
            CprBitemporalRecord existing = addresses.get(effectDate);
            if (existing == null || (addressDataRecord.getOriginDate() != null && !addressDataRecord.getOriginDate().isAfter(existing.getOriginDate()))) {
                addresses.put(effectDate, addressDataRecord);
            }
        }

        for (ForeignAddressEmigrationDataRecord emigrationDataRecord : sortRecords(person.getEmigration())) {
            OffsetDateTime effectDate = emigrationDataRecord.getEffectFrom();
            if (emigrationDataRecord.getEffectFrom() != null && Objects.equals(emigrationDataRecord.getEffectFrom(), emigrationDataRecord.getEffectTo())) {
                continue;
            }
            if (emigrationDataRecord.getReplacedby() != null) {
                continue;
            }
            if (isUndone(emigrationDataRecord)) {
                continue;
            }
            CprBitemporalRecord existing = addresses.get(effectDate);
            if (existing == null || (emigrationDataRecord.getOriginDate() != null && !emigrationDataRecord.getOriginDate().isAfter(existing.getOriginDate()))) {
                addresses.put(effectDate, emigrationDataRecord);
            }
        }

        ArrayList<OffsetDateTime> addressTimes = new ArrayList<>(addresses.keySet());
        HashMap<OffsetDateTime, Map<String, String>> moves = new HashMap<>();
        addressTimes.sort(Comparator.nullsFirst(OffsetDateTime::compareTo));

        int last = addressTimes.size() - 1;

        for (int i = 0; i <= last; i++) {
            OffsetDateTime previous = i > 0 ? addressTimes.get(i - 1) : null;
            OffsetDateTime current = addressTimes.get(i);
            CprBitemporalRecord currentAddress = addresses.get(current);
            CprBitemporalRecord previousAddress = addresses.get(previous);

            OffsetDateTime regFrom = currentAddress.getRegistrationFrom();
            CprBitemporalRecord c = (CprBitemporalRecord) currentAddress.getCorrectionof();
            if (c != null) {
                regFrom = c.getRegistrationFrom();
            }
            final OffsetDateTime firstRegFrom = regFrom;

            if (
                    (current != null && currentAddress.getRegistrationFrom() != null) &&
                            (filter.after == null || !current.isBefore(filter.after)) &&
                            (filter.before == null || !current.isAfter(filter.before)) &&
                            (filter.registrationAfter == null || !firstRegFrom.isBefore(filter.registrationAfter)) &&
                            (filter.registrationBefore == null || !firstRegFrom.isAfter(filter.registrationBefore)) &&
                            (filter.originAfter == null || !currentAddress.getOriginDate().isBefore(filter.originAfter)) &&
                            (filter.originBefore == null || !currentAddress.getOriginDate().isAfter(filter.originBefore))
                    ) {
                if (previousAddress == null || !isInGreenland(previousAddress) && !isInGreenland(currentAddress)) {
                    continue;
                }

                HashMap<String, String> item = new HashMap<>();
                item.put(PNR, formatPnr(person.getPersonnummer()));

                if (previousAddress != null) {

                    if (previousAddress instanceof AddressDataRecord) {
                        AddressDataRecord previousDomesticAddress = (AddressDataRecord) previousAddress;
                        item.put(ORIGIN_MUNICIPALITY_CODE, formatMunicipalityCode(previousDomesticAddress.getMunicipalityCode()));
                        item.put(ORIGIN_ROAD_CODE, formatRoadCode(previousDomesticAddress.getRoadCode()));
                        item.put(ORIGIN_HOUSE_NUMBER, formatHouseNnr(previousDomesticAddress.getHouseNumber()));
                        item.put(ORIGIN_FLOOR, formatFloor(previousDomesticAddress.getFloor()));
                        item.put(ORIGIN_DOOR_NUMBER, formatDoor(previousDomesticAddress.getDoor()));
                        item.put(ORIGIN_BNR, formatBnr(previousDomesticAddress.getBuildingNumber()));
                        GeoLookupDTO lookup = lookupService.doLookup(previousDomesticAddress.getMunicipalityCode(), previousDomesticAddress.getRoadCode());
                        item.put(ORIGIN_LOCALITY_NAME, lookup.getLocalityAbbrev());
                    }
                    if (previousAddress instanceof ForeignAddressEmigrationDataRecord) {
                        ForeignAddressEmigrationDataRecord previousMigration = (ForeignAddressEmigrationDataRecord) previousAddress;
                        item.put(ORIGIN_COUNTRY_CODE, Integer.toString(previousMigration.getImmigrationCountryCode()));
                    }
                }
                if (currentAddress != null) {
                    if (currentAddress instanceof AddressDataRecord) {
                        AddressDataRecord currentDomesticAddress = (AddressDataRecord) currentAddress;
                        item.put(DESTINATION_MUNICIPALITY_CODE, formatMunicipalityCode(currentDomesticAddress.getMunicipalityCode()));
                        item.put(DESTINATION_ROAD_CODE, formatRoadCode(currentDomesticAddress.getRoadCode()));
                        item.put(DESTINATION_HOUSE_NUMBER, formatHouseNnr(currentDomesticAddress.getHouseNumber()));
                        item.put(DESTINATION_FLOOR, formatFloor(currentDomesticAddress.getFloor()));
                        item.put(DESTINATION_DOOR_NUMBER, formatDoor(currentDomesticAddress.getDoor()));
                        item.put(DESTINATION_BNR, formatBnr(currentDomesticAddress.getBuildingNumber()));
                        item.put(MOVE_DATE, formatTime(current));
                        item.put(PROD_DATE, formatTime(firstRegFrom));
                        item.put(FILE_DATE, formatTime(currentAddress.getOriginDate()));

                        GeoLookupDTO lookup = lookupService.doLookup(currentDomesticAddress.getMunicipalityCode(), currentDomesticAddress.getRoadCode());
                        item.put(DESTINATION_LOCALITY_NAME, lookup.getLocalityAbbrev());
                    }
                    if (currentAddress instanceof ForeignAddressEmigrationDataRecord) {
                        ForeignAddressEmigrationDataRecord currentMigration = (ForeignAddressEmigrationDataRecord) currentAddress;
                        item.put(DESTINATION_COUNTRY_CODE, Integer.toString(currentMigration.getEmigrationCountryCode()));
                        item.put(MOVE_DATE, formatTime(current));
                        item.put(PROD_DATE, formatTime(currentAddress.getRegistrationFrom()));
                        item.put(FILE_DATE, formatTime(currentAddress.getOriginDate()));
                    }
                }

                //Make sure to only add the moving to the report if there is recieved a moving event at the same time
                if(eventListMove.stream().anyMatch(event -> event.getTimestamp().equals(firstRegFrom))) {
                    moves.put(current, item);
                }
            }
        }


        if (moves.isEmpty()) {
            return Collections.emptyList();
        }


        for (OffsetDateTime moveTime : moves.keySet()) {
            Map<String, String> item = moves.get(moveTime);
            // Important: Populate the appropriate map with data as is relevant at the time of moving
            for (BirthTimeDataRecord birthTimeDataRecord : sortRecords(filterRecordsByEffect(person.getBirthTime(), moveTime))) {
                if (birthTimeDataRecord.getBirthDatetime() != null) {
                    item.put(BIRTHDAY_YEAR, Integer.toString(birthTimeDataRecord.getBirthDatetime().getYear()));
                }
            }
            for (BirthPlaceDataRecord birthPlaceDataRecord : sortRecords(filterRecordsByEffect(person.getBirthPlace(), moveTime))) {
                item.put(BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
            }
            for (PersonStatusDataRecord statusDataRecord : sortRecords(filterRecordsByEffect(person.getStatus(), moveTime))) {
                item.put(STATUS_CODE, formatStatusCode(statusDataRecord.getStatus()));
            }
            for (CitizenshipDataRecord citizenshipDataRecord : sortRecords(filterRecordsByEffect(person.getCitizenship(), moveTime))) {
                item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
            }
            for (PersonNumberDataRecord personNumberDataRecord : sortRecords(filterRecordsByEffect(person.getPersonNumber(), moveTime))) {
                item.put(EFFECTIVE_PNR, formatPnr(personNumberDataRecord.getCprNumber()));
            }
            for (ParentDataRecord parentDataRecord : sortRecords(filterRecordsByEffect(person.getMother(), moveTime))) {
                item.put(MOTHER_PNR, formatPnr(parentDataRecord.getCprNumber()));
            }
            for (ParentDataRecord parentDataRecord : sortRecords(filterRecordsByEffect(person.getFather(), moveTime))) {
                item.put(FATHER_PNR, formatPnr(parentDataRecord.getCprNumber()));
            }
            for (CivilStatusDataRecord civilStatusDataRecord : sortRecords(filterRecordsByEffect(person.getCivilstatus(), moveTime))) {
                item.put(SPOUSE_PNR, formatPnr(civilStatusDataRecord.getSpouseCpr()));
            }

            replaceMapValues(item, null, "");
        }

        return new ArrayList<>(moves.values());
    }

    private static boolean isInGreenland(CprBitemporalRecord addressData) {
        boolean glFound = false;
        if (addressData != null) {
            if (addressData instanceof AddressDataRecord) {
                AddressDataRecord previousDomesticAddress = (AddressDataRecord) addressData;
                if (previousDomesticAddress.getMunicipalityCode() > 900) {
                    glFound = true;
                }
            }
        }
        return glFound;
    }

    private static boolean isUndone(CprNontemporalRecord record) {
        if (record.getReplacedby() != null) {
            CprNontemporalRecord replacement = record;
            for (int stopper = 100; replacement != null && stopper > 0; stopper--) {
                replacement = replacement.getReplacedby();
                if (replacement != null) {
                    if (replacement.isUndone()) {
                        return true;
                    }
                }
            }
        }
        return false;
    }
}