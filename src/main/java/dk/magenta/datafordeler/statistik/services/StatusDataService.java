package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.ListHashMap;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonStatusQuery;
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
import java.time.OffsetDateTime;
import java.util.*;


/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/status_data")
public class StatusDataService extends StatisticsService {

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
    protected String[] requiredParameters() {
        return new String[]{StatisticsService.EFFECT_DATE_PARAMETER};
    }

    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void handleRequest(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.handleRequest(request, response, ServiceName.STATUS);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, FIRST_NAME, LAST_NAME, STATUS_CODE,
                BIRTH_AUTHORITY, BIRTH_AUTHORITY_TEXT, CITIZENSHIP_CODE, MOTHER_PNR, FATHER_PNR, CIVIL_STATUS, SPOUSE_PNR,
                MUNICIPALITY_CODE, LOCALITY_NAME, LOCALITY_CODE, LOCALITY_ABBREVIATION, ROAD_CODE, HOUSE_NUMBER, FLOOR_NUMBER, DOOR_NUMBER,
                BNR, MOVING_IN_DATE, MOVE_PROD_DATE, POST_CODE, CIVIL_STATUS_DATE, CIVIL_STATUS_PROD_DATE, CHURCH
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
    protected PersonQuery getQuery(Filter filter) {
        PersonStatusQuery personStatusQuery = new PersonStatusQuery();
        OffsetDateTime livingInGreenlandOnDate = filter.effectAt;
        if (livingInGreenlandOnDate != null) {
            personStatusQuery.setLivingInGreenlandOn(livingInGreenlandOnDate);
        }
        if (filter.onlyPnr != null) {
            for (String pnr : filter.onlyPnr) {
                personStatusQuery.addPersonnummer(pnr);
            }
        }
        personStatusQuery.setPageSize(1000000);
        return personStatusQuery;
    }
    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        return Collections.singletonList(this.formatPersonByRecord(person, session, lookupService, filter));
    }

    protected List<Map<String, String>> formatPersonByRVD(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();

        item.put(PNR, person.getPersonnummer());

        OffsetDateTime latestCivilStatusDate = null;
        OffsetDateTime civilStatusRegistrationDate = null;
        OffsetDateTime latestAddressTime = null;
        PersonAddressData latestAddress = null;

        // Loop over the list of registrations (which is already sorted (by time, ascending))
        for (PersonRegistration registration : person.getRegistrations()){
            List<PersonEffect> effects = registration.getEffectsAt(filter.effectAt);
            effects.sort(this.personComparator);
            for (PersonEffect effect : effects) {
                for (PersonBaseData data : effect.getDataItems()) {

                        PersonNameData nameData = data.getName();
                        if (nameData != null) {
                            item.put(FIRST_NAME, nameData.getFirstNames());
                            item.put(LAST_NAME, nameData.getLastName());
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
                                item.put(BIRTH_AUTHORITY_CODE_TEXT, Integer.toString(birthData.getBirthAuthorityText()));
                            }
                            if (birthData.getBirthSupplementalText() != null) {
                                item.put(BIRTH_AUTHORITY_TEXT, birthData.getBirthSupplementalText());
                            }
                        }


                        PersonStatusData statusData = data.getStatus();
                        if (statusData != null) {
                            item.put(STATUS_CODE, formatStatusCode(statusData.getStatus()));
                        }


                        PersonCitizenshipData citizenshipData = data.getCitizenship();
                        if (citizenshipData != null) {
                            item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipData.getCountryCode()));
                        }


                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null && effect.getEffectFrom() != null) {
                        if (latestAddressTime == null || latestAddressTime.isBefore(effect.getEffectFrom()) ||
                                (latestAddressTime.isEqual(effect.getEffectFrom()) && (latestAddress == null || latestAddress.getId() < addressData.getId()))) {
                            latestAddressTime = effect.getEffectFrom();
                            latestAddress = addressData;
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


                    // We can't skip this if it's already set; we need the registrationTime
                    PersonCivilStatusData personCivilStatus = data.getCivilStatus();
                    if (personCivilStatus != null) {
                        item.put(CIVIL_STATUS, personCivilStatus.getCivilStatus());
                        if (effect.getEffectFrom() != null && (latestCivilStatusDate == null || effect.getEffectFrom().isAfter(latestCivilStatusDate))) {
                            latestCivilStatusDate = effect.getEffectFrom();
                            civilStatusRegistrationDate = registration.getRegistrationFrom();
                        }
                    }

                        PersonCivilStatusData personSpouseData = data.getCivilStatus();
                        if (personSpouseData != null) {
                            item.put(SPOUSE_PNR, personSpouseData.getSpouseCpr());
                        }


                        PersonChurchData personChurchData = data.getChurch();
                        if (personChurchData != null) {
                            item.put(CHURCH, personChurchData.getChurchRelation().toString());
                        }

                }
            }
        }
        if (latestCivilStatusDate != null) {
            item.put(CIVIL_STATUS_DATE, latestCivilStatusDate.format(dmyFormatter));
        }
        if (civilStatusRegistrationDate != null) {
            item.put(CIVIL_STATUS_PROD_DATE, civilStatusRegistrationDate.format(dmyFormatter));
        }

        if (latestAddress != null) {
            item.put(POST_CODE, latestAddress.getPostalCode());
            item.put(MUNICIPALITY_CODE, Integer.toString(latestAddress.getMunicipalityCode()));
            item.put(ROAD_CODE, formatRoadCode(latestAddress.getRoadCode()));
            item.put(HOUSE_NUMBER, formatHouseNnr(latestAddress.getHouseNumber()));
            item.put(DOOR_NUMBER, latestAddress.getDoor());
            item.put(BNR, formatBnr(latestAddress.getBuildingNumber()));
            item.put(FLOOR_NUMBER,latestAddress.getFloor());

            // Use the lookup service to extract locality & postcode data from a municipality code and road code
            Lookup lookup = lookupService.doLookup(
                    latestAddress.getMunicipalityCode(),
                    latestAddress.getRoadCode(),
                    latestAddress.getHouseNumber()
            );
            if (lookup != null) {
                item.put(LOCALITY_NAME, lookup.localityName);
                item.put(LOCALITY_CODE, formatLocalityCode(lookup.localityCode));
                item.put(LOCALITY_ABBREVIATION, lookup.localityAbbrev);
                item.put(POST_CODE, Integer.toString(lookup.postalCode));
            }

            // We're looking for a persons newest address, as well as the time it was first registered
            // So, populate these two structures:
            // Map of effectTime to addresses (when address was moved into)
            ListHashMap<OffsetDateTime, PersonAddressData> addresses = new ListHashMap<>();
            // Map of effectTime to registrationTime (when this move was *first* registered)
            HashMap<OffsetDateTime, OffsetDateTime> addressRegistrationTimes = new HashMap<>();

            for (PersonRegistration registration : person.getRegistrations()) {
                for (PersonEffect effect : registration.getEffects()) {
                    OffsetDateTime effectTime = effect.getEffectFrom();
                    for (PersonBaseData data : effect.getDataItems()) {
                        PersonAddressData addressData = data.getAddress();
                        if (addressData != null) {
                            addresses.add(effectTime, addressData);
                            if (!addressRegistrationTimes.containsKey(effectTime)) {
                                OffsetDateTime oldTime = addressRegistrationTimes.get(effectTime);
                                OffsetDateTime newTime = registration.getRegistrationFrom();
                                if (newTime != null && (oldTime == null || newTime.isBefore(oldTime))) {
                                    addressRegistrationTimes.put(effectTime, newTime);
                                }
                            }
                        }
                    }
                }
            }
            ArrayList<OffsetDateTime> addressTimes = new ArrayList<>(addresses.keySet());
            addressTimes.sort(Comparator.nullsFirst(OffsetDateTime::compareTo));
            for (OffsetDateTime addressTime : addressTimes) {
                if (addresses.get(addressTime).contains(latestAddress)) {
                    item.put(MOVING_IN_DATE, addressTime.format(dmyFormatter));
                    item.put(MOVE_PROD_DATE, addressRegistrationTimes.get(addressTime) != null ? addressRegistrationTimes.get(addressTime).format(dmyFormatter) : null);
                    break;
                }
            }
        }
        return Collections.singletonList(item);
    }

    protected Map<String, String> formatPersonByRecord(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();
        item.put(PNR, formatPnr(person.getPersonnummer()));

        // Loop over the list of registrations (which is already sorted (by time, ascending))
        for (NameDataRecord nameDataRecord : sortRecords(filterRecordsByEffect(person.getName(), filter.effectAt))) {
            item.put(FIRST_NAME, nameDataRecord.getFirstNames());
            item.put(LAST_NAME, nameDataRecord.getLastName());
        }

        for (BirthPlaceDataRecord birthPlaceDataRecord : sortRecords(filterRecordsByEffect(person.getBirthPlace(), filter.effectAt))) {
            item.put(BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
            item.put(BIRTH_AUTHORITY_CODE_TEXT, birthPlaceDataRecord.getBirthPlaceName());
            item.put(BIRTH_AUTHORITY_TEXT, birthPlaceDataRecord.getBirthPlaceName());
        }
        for (BirthTimeDataRecord birthTimeDataRecord : sortRecords(filterRecordsByEffect(person.getBirthTime(), filter.effectAt))) {
            item.put(BIRTHDAY_YEAR, Integer.toString(birthTimeDataRecord.getBirthDatetime().getYear()));
        }
        for (PersonStatusDataRecord statusDataRecord : sortRecords(filterRecordsByEffect(person.getStatus(), filter.effectAt))) {
            item.put(STATUS_CODE, formatStatusCode(statusDataRecord.getStatus()));
        }
        for (CitizenshipDataRecord citizenshipDataRecord : sortRecords(filterRecordsByEffect(person.getCitizenship(), filter.effectAt))) {
            item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
        }
        for (ParentDataRecord parentDataRecord : sortRecords(filterRecordsByEffect(person.getMother(), filter.effectAt))) {
            item.put(MOTHER_PNR, formatPnr(parentDataRecord.getCprNumber()));
        }
        for (ParentDataRecord parentDataRecord : sortRecords(filterRecordsByEffect(person.getFather(), filter.effectAt))) {
            item.put(FATHER_PNR, formatPnr(parentDataRecord.getCprNumber()));
        }
        for (CivilStatusDataRecord civilStatusDataRecord : sortRecords(filterRecordsByEffect(person.getCivilstatus(), filter.effectAt))) {
            item.put(SPOUSE_PNR, formatPnr(civilStatusDataRecord.getSpouseCpr()));
        }
        for (ChurchDataRecord churchDataRecord : sortRecords(filterRecordsByEffect(person.getChurchRelation(), filter.effectAt))) {
            item.put(CHURCH, churchDataRecord.getChurchRelation().toString());
        }

        for (CivilStatusDataRecord civilStatusDataRecord : sortRecords(filterRecordsByEffect(person.getCivilstatus(), filter.effectAt))) {
            item.put(CIVIL_STATUS, civilStatusDataRecord.getCivilStatus());
            item.put(CIVIL_STATUS_DATE, formatTime(civilStatusDataRecord.getEffectFrom()));
            item.put(CIVIL_STATUS_PROD_DATE, formatTime(civilStatusDataRecord.getRegistrationFrom()));
        }

        for (AddressDataRecord addressDataRecord : sortRecords(filterRecordsByEffect(person.getAddress(), filter.effectAt))) {
            item.put(MOVING_IN_DATE, formatTime(addressDataRecord.getEffectFrom()));
            item.put(MOVE_PROD_DATE, formatTime(addressDataRecord.getRegistrationFrom()));

            item.put(MUNICIPALITY_CODE, Integer.toString(addressDataRecord.getMunicipalityCode()));
            item.put(ROAD_CODE, formatRoadCode(addressDataRecord.getRoadCode()));
            item.put(HOUSE_NUMBER, formatHouseNnr(addressDataRecord.getHouseNumber()));
            item.put(DOOR_NUMBER, addressDataRecord.getDoor());
            item.put(BNR, formatBnr(addressDataRecord.getBuildingNumber()));
            item.put(FLOOR_NUMBER,addressDataRecord.getFloor());

            // Use the lookup service to extract locality & postcode data from a municipality code and road code
            Lookup lookup = lookupService.doLookup(
                    addressDataRecord.getMunicipalityCode(),
                    addressDataRecord.getRoadCode(),
                    addressDataRecord.getHouseNumber()
            );
            if (lookup != null) {
                item.put(LOCALITY_NAME, lookup.localityName);
                item.put(LOCALITY_CODE, formatLocalityCode(lookup.localityCode));
                item.put(LOCALITY_ABBREVIATION, lookup.localityAbbrev);
                item.put(POST_CODE, Integer.toString(lookup.postalCode));
            }
        }

        return item;
    }
}
