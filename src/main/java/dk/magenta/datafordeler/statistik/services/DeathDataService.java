package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonDeathQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.Lookup;
import dk.magenta.datafordeler.statistik.utils.LookupService;
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
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/death_data")
public class DeathDataService extends PersonStatisticsService {
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

    private Logger log = LogManager.getLogger(DeathDataService.class.getCanonicalName());


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.DEATH);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                STATUS_CODE, DEATH_DATE, PROD_DATE, FILE_DATE, PNR, BIRTHDAY_YEAR,
                MOTHER_PNR, FATHER_PNR, SPOUSE_PNR,
                EFFECTIVE_PNR, CITIZENSHIP_CODE, BIRTH_AUTHORITY, BIRTH_AUTHORITY_TEXT, MUNICIPALITY_CODE,
                LOCALITY_NAME, LOCALITY_ABBREVIATION, LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, DOOR_NUMBER, BNR
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
        return new PersonDeathQuery(filter);
    }


    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        Map<String, String> itemMap = this.formatPersonByRecord(person, session, lookupService, filter);
        if (itemMap == null || itemMap.isEmpty()) {
            return Collections.emptyList();
        }
        HashMap<String, String> item = new HashMap<>(itemMap);
        item.put(PNR, person.getPersonnummer());
        return Collections.singletonList(item);
    }

    protected Map<String, String> formatPersonByRecord(PersonEntity person, Session session, LookupService lookupService, Filter filter) {

        HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());

        OffsetDateTime deathEffectTime = null;
        OffsetDateTime deathRegistrationTime = null;
        LocalDate deathFileTime = null;
        for (PersonStatusDataRecord statusDataRecord : sortRecords(person.getStatus())) {
            if (statusDataRecord.getBitemporality().registrationTo == null) {
                item.put(STATUS_CODE, Integer.toString(statusDataRecord.getStatus()));
                if (statusDataRecord.getStatus() == 90) {
                    OffsetDateTime thisdeathEffectTime = statusDataRecord.getEffectFrom();
                    if (deathEffectTime == null || thisdeathEffectTime == null || thisdeathEffectTime.isBefore(deathEffectTime)) {
                        deathEffectTime = thisdeathEffectTime;
                    }
                    OffsetDateTime thisDeathRegistrationTime = statusDataRecord.getRegistrationFrom();
                    if (deathRegistrationTime == null || thisDeathRegistrationTime == null || thisDeathRegistrationTime.isBefore(deathRegistrationTime)) {
                        deathRegistrationTime = thisDeathRegistrationTime;
                    }

                    LocalDate thisDeathFileTime = statusDataRecord.getOriginDate();
                    if (deathFileTime == null || thisDeathFileTime.isBefore(deathFileTime)) {
                        deathFileTime = thisDeathFileTime;
                    }
                }
            }
        }

        if (
                deathEffectTime == null ||
                        (filter.after != null && deathEffectTime.isBefore(filter.after)) ||
                        (filter.registrationAfter != null && deathRegistrationTime.isBefore(filter.registrationAfter))
                ) {
            return Collections.emptyMap();
        }

        if (deathEffectTime != null) {
            item.put(DEATH_DATE, formatTime(deathEffectTime.atZoneSameInstant(cprDataOffset)));
        }
        if (deathRegistrationTime != null) {
            item.put(PROD_DATE, formatTime(deathRegistrationTime.atZoneSameInstant(cprDataOffset)));
        }
        if (deathFileTime != null) {
            item.put(FILE_DATE, formatTime(deathFileTime));
        }

        filter.effectAt = deathEffectTime;


        for (PersonNumberDataRecord personNumberDataRecord : sortRecords(person.getPersonNumber())) {
            if (personNumberDataRecord.getBitemporality().registrationTo == null && personNumberDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                item.put(EFFECTIVE_PNR, personNumberDataRecord.getCprNumber());
            }
        }

        for (BirthPlaceDataRecord birthPlaceDataRecord : sortRecords(person.getBirthPlace())) {
            if (birthPlaceDataRecord.getBitemporality().registrationTo == null && birthPlaceDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                item.put(BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
                item.put(BIRTH_AUTHORITY_TEXT, birthPlaceDataRecord.getBirthPlaceName());
                item.put(BIRTH_AUTHORITY_CODE_TEXT, Integer.toString(birthPlaceDataRecord.getBirthPlaceCode()));
            }
        }
        for (BirthTimeDataRecord birthTimeDataRecord : sortRecords(person.getBirthTime())) {
            if (birthTimeDataRecord.getBitemporality().registrationTo == null && birthTimeDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                LocalDateTime birthDatetime = birthTimeDataRecord.getBirthDatetime();
                if (birthDatetime != null) {
                    item.put(BIRTHDAY_YEAR, Integer.toString(birthDatetime.getYear()));
                }
            }
        }

        for (CitizenshipDataRecord citizenshipDataRecord : sortRecords(person.getCitizenship())) {
            if (citizenshipDataRecord.getBitemporality().registrationTo == null && citizenshipDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
            }
        }

        int municipalityCode = 0;
        for (AddressDataRecord addressDataRecord : sortRecords(person.getAddress())) {
            if (addressDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime) && addressDataRecord.getReplacedby() == null) {
                municipalityCode = addressDataRecord.getMunicipalityCode();
                item.put(MUNICIPALITY_CODE, Integer.toString(municipalityCode));
                item.put(ROAD_CODE, formatRoadCode(addressDataRecord.getRoadCode()));
                item.put(HOUSE_NUMBER, formatHouseNnr(addressDataRecord.getHouseNumber()));
                item.put(FLOOR_NUMBER, addressDataRecord.getFloor());
                item.put(DOOR_NUMBER, addressDataRecord.getDoor());
                item.put(BNR, formatBnr(addressDataRecord.getBuildingNumber()));
                Lookup lookup = lookupService.doLookup(
                        addressDataRecord.getMunicipalityCode(),
                        addressDataRecord.getRoadCode(),
                        addressDataRecord.getHouseNumber()
                );
                if (lookup != null) {
                    if (lookup.localityName != null) {
                        item.put(LOCALITY_NAME, lookup.localityName);
                    }
                    if (lookup.localityAbbrev != null) {
                        item.put(LOCALITY_ABBREVIATION, lookup.localityAbbrev);
                    }
                    if (lookup.localityCode != 0) {
                        item.put(LOCALITY_CODE, formatLocalityCode(lookup.localityCode));
                    }
                }
            }
        }
        if (municipalityCode < 955 || municipalityCode > 961) {
            return null;
        }

        for (ParentDataRecord motherRecord : sortRecords(person.getMother())) {
            if (motherRecord.getBitemporality().registrationTo == null && motherRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                item.put(MOTHER_PNR, motherRecord.getCprNumber());
            }
        }
        for (ParentDataRecord fatherRecord : sortRecords(person.getFather())) {
            if (fatherRecord.getBitemporality().registrationTo == null && fatherRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                item.put(FATHER_PNR, fatherRecord.getCprNumber());
            }
        }
        for (CivilStatusDataRecord civilStatusDataRecord : sortRecords(person.getCivilstatus())) {
            if (civilStatusDataRecord.getBitemporality().registrationTo == null && civilStatusDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                item.put(SPOUSE_PNR, civilStatusDataRecord.getSpouseCpr());
            }
        }

        replaceMapValues(item, null, "");
        return item;
    }

}