package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonDeathQuery;
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
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.*;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/death_data")
public class DeathDataService extends StatisticsService {
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

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.handleRequest(request, response, ServiceName.DEATH);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                STATUS_CODE , DEATH_DATE, PROD_DATE, PNR, BIRTHDAY_YEAR,
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
    protected PersonQuery getQuery(Filter filter) {
        return new PersonDeathQuery(filter);
    }


    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>(this.formatPersonByRecord(person, session, lookupService, filter));
        if (item.isEmpty()) {
            return Collections.emptyList();
        }
        item.put(PNR, person.getPersonnummer());
        return Collections.singletonList(item);
    }

    protected List<Map<String, String>> formatPersonByRVD(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());

        OffsetDateTime earliestProdDate = null;
        OffsetDateTime earliestDeathTime = null;


        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffects()) {
                for (PersonBaseData data : effect.getDataItems()) {
                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put(STATUS_CODE, Integer.toString(statusData.getStatus()));
                        if (statusData.getStatus() == 90) {
                            if (effect.getEffectFrom() != null && (earliestDeathTime == null || effect.getEffectFrom().isBefore(earliestDeathTime))) {
                                earliestDeathTime = effect.getEffectFrom();
                            }
                            if (registration.getRegistrationFrom() != null && (earliestProdDate == null || registration.getRegistrationFrom().isBefore(earliestProdDate))) {
                                earliestProdDate = registration.getRegistrationFrom();
                            }
                        }
                    }
                }
            }
        }

        if (
                earliestDeathTime == null ||
                (filter.after != null && earliestDeathTime.isBefore(filter.after)) ||
                (filter.registrationAfter != null && earliestProdDate.isBefore(filter.registrationAfter))
        ) {
            return Collections.emptyList();
        }

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffects()) {
                //for (PersonEffect effect: registration.getEffectsAt(earliestDeathTime)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonCoreData coreData = data.getCoreData();

                    if (coreData != null) {
                        item.put(EFFECTIVE_PNR, coreData.getCprNumber());
                    }


                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthDatetime() != null) {
                            item.put(BIRTHDAY_YEAR, Integer.toString(birthData.getBirthDatetime().getYear()));
                        }
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(BIRTH_AUTHORITY, Integer.toString(birthData.getBirthPlaceCode()));
                        }
                        if (birthData.getBirthAuthorityText() != null) {
                            item.put(BIRTH_AUTHORITY_TEXT, Integer.toString(birthData.getBirthAuthorityText()));
                        }
                    }



                    PersonCitizenshipData citizenshipData = data.getCitizenship();
                    if (citizenshipData != null) {
                        item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipData.getCountryCode()));
                    }

                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        item.put(ROAD_CODE, formatRoadCode(addressData.getRoadCode()));
                        item.put(HOUSE_NUMBER, formatHouseNnr(addressData.getHouseNumber()));
                        item.put(DOOR_NUMBER, addressData.getDoor());
                        item.put(BNR, formatBnr(addressData.getBuildingNumber()));
                        item.put(MUNICIPALITY_CODE, Integer.toString(addressData.getMunicipalityCode()));
                        Lookup lookup = lookupService.doLookup(
                                addressData.getMunicipalityCode(),
                                addressData.getRoadCode(),
                                addressData.getHouseNumber()
                        );
                        if (lookup != null) {
                            item.put(LOCALITY_NAME, lookup.localityName);
                            item.put(LOCALITY_ABBREVIATION, lookup.localityAbbrev);
                            item.put(LOCALITY_CODE, formatLocalityCode(lookup.localityCode));
                        }
                    }

                    PersonParentData personMotherData = data.getMother();
                    if (personMotherData != null) {
                        item.put(MOTHER_PNR, personMotherData.getCprNumber());
                    }

                    PersonParentData personFatherData = data.getFather();
                    if (personFatherData != null) {
                        item.put(FATHER_PNR, personFatherData.getCprNumber());
                    }

                    PersonCivilStatusData personCivilStatusData = data.getCivilStatus();
                    if (personCivilStatusData != null) {
                        item.put(SPOUSE_PNR, personCivilStatusData.getSpouseCpr());
                    }

                }
            }
        }
        if (earliestDeathTime != null) {
            item.put(DEATH_DATE, earliestDeathTime.atZoneSameInstant(cprDataOffset).format(dmyFormatter));
        }
        if (earliestProdDate != null) {
            item.put(PROD_DATE, earliestProdDate.atZoneSameInstant(cprDataOffset).format(dmyFormatter));
        }

        return Collections.singletonList(item);
    }



    protected Map<String, String> formatPersonByRecord(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();

        item.put(PNR, person.getPersonnummer());
        
        OffsetDateTime deathEffectTime = null;
        OffsetDateTime deathRegistrationTime = null;
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

        for (AddressDataRecord addressDataRecord : sortRecords(person.getAddress())) {
            item.put(MUNICIPALITY_CODE, Integer.toString(addressDataRecord.getMunicipalityCode()));
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

        return item;
    }

}