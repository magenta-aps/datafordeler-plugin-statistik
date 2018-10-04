package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
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
import dk.magenta.datafordeler.statistik.queries.PersonBirthQuery;
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
@RequestMapping("/statistik/birth_data")
public class BirthDataService extends StatisticsService {

    private class Exclude extends Exception {
    }

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


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void handleRequest(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        super.handleRequest(request, response, ServiceName.BIRTH);
    }

    private static final String OWN_PREFIX = "B_";

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                OWN_PREFIX + PNR, OWN_PREFIX + BIRTHDAY_YEAR, OWN_PREFIX + EFFECTIVE_PNR, OWN_PREFIX + BIRTH_AUTHORITY, OWN_PREFIX + BIRTH_AUTHORITY_TEXT, OWN_PREFIX + BIRTH_AUTHORITY_CODE_TEXT, OWN_PREFIX + CITIZENSHIP_CODE, OWN_PREFIX + PROD_DATE,
                MOTHER_PREFIX + PNR, MOTHER_PREFIX + BIRTH_AUTHORITY, MOTHER_PREFIX + BIRTH_AUTHORITY_TEXT, MOTHER_PREFIX + BIRTH_AUTHORITY_CODE_TEXT, MOTHER_PREFIX + CITIZENSHIP_CODE, MOTHER_PREFIX + MUNICIPALITY_CODE, MOTHER_PREFIX + LOCALITY_NAME, MOTHER_PREFIX + LOCALITY_CODE, MOTHER_PREFIX + ROAD_CODE, MOTHER_PREFIX + HOUSE_NUMBER, MOTHER_PREFIX + FLOOR_NUMBER, MOTHER_PREFIX + DOOR_NUMBER, MOTHER_PREFIX + BNR,
                FATHER_PREFIX + PNR, FATHER_PREFIX + BIRTH_AUTHORITY, FATHER_PREFIX + BIRTH_AUTHORITY_TEXT, FATHER_PREFIX + BIRTH_AUTHORITY_CODE_TEXT, FATHER_PREFIX + CITIZENSHIP_CODE, FATHER_PREFIX + MUNICIPALITY_CODE, FATHER_PREFIX + LOCALITY_NAME, FATHER_PREFIX + LOCALITY_CODE, FATHER_PREFIX + ROAD_CODE, FATHER_PREFIX + HOUSE_NUMBER, FATHER_PREFIX + FLOOR_NUMBER, FATHER_PREFIX + DOOR_NUMBER, FATHER_PREFIX + BNR
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
        return new PersonBirthQuery(filter);
    }



/*
    //---
    @Override
    protected List<PersonQuery> getQueryList(HttpServletRequest request) throws IOException {
        //The name or path of the file must be here
        File inFile = new File("C:\\Users\\EFRIN.GONZALEZ\\Downloads\\inFile.csv");
        //String inFile = "/home/lars/tmp/foo.txt";
        ArrayList<String> pnrs = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.handleRequest(inFile.toString()))) {
            stream.forEach(pnrs::add);
        }
        System.out.println(pnrs.size() + " pnrs loaded");

        int count = 0;
        ArrayList<PersonQuery> queries = new ArrayList<>();
        PersonQuery personQuery = new PersonQuery();
        for (String pnr : pnrs) {
            count++;
            personQuery.addPersonnummer(pnr);
            //TODO: What's the rationale behind this if condition? the 1000 is related to ?
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

*/


    //---



    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>(this.formatPersonByRecord(person, session, lookupService, filter));
        if (item.isEmpty()) {
            return Collections.emptyList();
        }
        item.put(OWN_PREFIX + PNR, person.getPersonnummer());
        return Collections.singletonList(item);
    }


    protected Map<String, String> formatPersonByRecord(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();
        item.put(OWN_PREFIX + PNR, person.getPersonnummer());

        OffsetDateTime birthEffectTime = null;
        OffsetDateTime birthRegistrationTime = null;
        for (BirthTimeDataRecord birthTimeDataRecord : sortRecords(person.getBirthTime())) {
            if (birthTimeDataRecord.getBitemporality().registrationTo == null) {
                LocalDateTime birthDatetime = birthTimeDataRecord.getBirthDatetime();
                if (birthDatetime != null) {
                    OffsetDateTime thisBirthEffectTime = birthTimeDataRecord.getEffectFrom();
                    if (birthEffectTime == null || thisBirthEffectTime == null || thisBirthEffectTime.isBefore(birthEffectTime)) {
                        birthEffectTime = thisBirthEffectTime;
                    }
                    OffsetDateTime thisBirthRegistrationTime = birthTimeDataRecord.getRegistrationFrom();
                    if (birthRegistrationTime == null || thisBirthRegistrationTime == null || thisBirthRegistrationTime.isBefore(birthRegistrationTime)) {
                        birthRegistrationTime = thisBirthRegistrationTime;
                    }
                }
            }
        }


        if (birthRegistrationTime != null) {
            item.put(OWN_PREFIX + PROD_DATE, formatTime(birthRegistrationTime));
        }


        ///////
        for (BirthPlaceDataRecord birthPlaceDataRecord : sortRecords(person.getBirthPlace())) {
            if (birthPlaceDataRecord.getBitemporality().registrationTo == null && birthPlaceDataRecord.getBitemporality().containsEffect(birthEffectTime, birthEffectTime)) {
                item.put(OWN_PREFIX + BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
                item.put(OWN_PREFIX + BIRTH_AUTHORITY_TEXT, birthPlaceDataRecord.getBirthPlaceName());
                item.put(OWN_PREFIX + BIRTH_AUTHORITY_CODE_TEXT, Integer.toString(birthPlaceDataRecord.getBirthPlaceCode()));
            }
        }
        LocalDateTime birthTime = null;
        for (BirthTimeDataRecord birthTimeDataRecord : sortRecords(person.getBirthTime())) {
            if (birthTimeDataRecord.getBitemporality().registrationTo == null && birthTimeDataRecord.getBitemporality().containsEffect(birthEffectTime, birthEffectTime)) {
                LocalDateTime birthDatetime = birthTimeDataRecord.getBirthDatetime();
                if (birthDatetime != null) {
                    item.put(OWN_PREFIX + BIRTHDAY_YEAR, Integer.toString(birthDatetime.getYear()));
                    birthTime = birthDatetime;
                }
            }
        }
        for (PersonNumberDataRecord personNumberDataRecord : sortRecords(person.getPersonNumber())) {
            if (personNumberDataRecord.getBitemporality().registrationTo == null && personNumberDataRecord.getBitemporality().containsEffect(birthEffectTime, birthEffectTime)) {
                item.put(OWN_PREFIX + EFFECTIVE_PNR, personNumberDataRecord.getCprNumber());
            }
        }
        for (CitizenshipDataRecord citizenshipDataRecord : sortRecords(person.getCitizenship())) {
            if (citizenshipDataRecord.getBitemporality().registrationTo == null && citizenshipDataRecord.getBitemporality().containsEffect(birthEffectTime, birthEffectTime)) {
                item.put(OWN_PREFIX + CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
            }
        }
        Filter parentFilter = new Filter(
                birthTime != null ?
                        birthTime.atZone(StatisticsService.cprDataOffset).toOffsetDateTime() :
                        birthRegistrationTime
        );

        String motherPnr = null;
        for (ParentDataRecord motherRecord : sortRecords(person.getMother())) {
            if (motherRecord.getBitemporality().registrationTo == null && motherRecord.getBitemporality().containsEffect(birthEffectTime, birthEffectTime)) {
                motherPnr = motherRecord.getCprNumber();
            }
        }
        item.put(MOTHER_PREFIX + PNR, motherPnr);
        if (motherPnr != null) {
            PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(motherPnr), PersonEntity.class);
            if (mother != null) {
                try {
                    item.putAll(this.formatParentPersonByRecord(mother, MOTHER_PREFIX, lookupService, parentFilter, true));
                } catch (Exclude e) {
                    // Do not include births where the mother lives outside of Greenland at the time of birth
                    return Collections.emptyMap();
                }
            }
        }

        String fatherPnr = null;
        for (ParentDataRecord fatherRecord : sortRecords(person.getFather())) {
            if (fatherRecord.getBitemporality().registrationTo == null && fatherRecord.getBitemporality().containsEffect(birthEffectTime, birthEffectTime)) {
                fatherPnr = fatherRecord.getCprNumber();
            }
        }
        item.put(FATHER_PREFIX + PNR, fatherPnr);
        if (fatherPnr != null) {
            PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(fatherPnr), PersonEntity.class);
            if (father != null) {
                try {
                    item.putAll(this.formatParentPersonByRecord(father, FATHER_PREFIX, lookupService, parentFilter, true));
                } catch (Exclude e) {
                    e.printStackTrace();
                }
            }
        }


        return item;
    }

    protected Map<String, String> formatPersonByRVD(PersonEntity person, Session session, LookupService lookupService, Filter filter) {

        HashMap<String, String> item = new HashMap<>();
        item.put(OWN_PREFIX + PNR, person.getPersonnummer());


        OffsetDateTime earliestProdDate = null;
        LocalDateTime birthTime = null;
        String motherPnr = null;
        String fatherPnr = null;

        OffsetDateTime earliestBirthTime = null;

        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffects()) {
                for (PersonBaseData data : effect.getDataItems()) {
                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthDatetime() != null) {
                            OffsetDateTime birthDateTime = birthData.getBirthDatetime().atZone(cprDataOffset).toOffsetDateTime();
                            if (earliestBirthTime == null || birthDateTime.isBefore(earliestBirthTime)) {
                                earliestBirthTime = birthDateTime;
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
                earliestProdDate == null ||
                (filter.registrationAfter != null && earliestProdDate.isBefore(filter.registrationAfter)) ||
                (filter.after != null && earliestBirthTime.isBefore(filter.after))
        ) {
            return Collections.emptyMap();
        }

        HashSet<PersonEffect> personEffects = new HashSet<>();
        for (PersonRegistration registration: person.getRegistrations()) {
            personEffects.addAll(registration.getEffectsAt(earliestBirthTime));
        }

        ArrayList<PersonEffect> effects = new ArrayList<>(personEffects);
        effects.sort(Comparator.nullsFirst(this.personComparator));

        for (PersonEffect effect: effects) {
            for (PersonBaseData data : effect.getDataItems()) {

                PersonCoreData coreData = data.getCoreData();
                if (coreData != null) {
                    item.put(OWN_PREFIX + EFFECTIVE_PNR, coreData.getCprNumber());
                }

                PersonBirthData birthData = data.getBirth();
                if (birthData != null) {
                    if (birthData.getBirthDatetime() != null) {
                        item.put(OWN_PREFIX + BIRTHDAY_YEAR, Integer.toString(birthData.getBirthDatetime().getYear()));
                        birthTime = birthData.getBirthDatetime();
                    }
                    if (birthData.getBirthPlaceCode() != null) {
                        item.put(OWN_PREFIX + BIRTH_AUTHORITY, Integer.toString(birthData.getBirthPlaceCode()));
                    }
                    if (birthData.getBirthAuthorityText() != null) {
                        item.put(OWN_PREFIX + BIRTH_AUTHORITY_CODE_TEXT, Integer.toString(birthData.getBirthAuthorityText()));
                    }
                    if (birthData.getBirthSupplementalText() != null) {
                        item.put(OWN_PREFIX + BIRTH_AUTHORITY_TEXT, birthData.getBirthSupplementalText());
                    }
                }

                PersonCitizenshipData citizenshipData = data.getCitizenship();
                if (citizenshipData != null) {
                    item.put(OWN_PREFIX + CITIZENSHIP_CODE, Integer.toString(citizenshipData.getCountryCode()));
                }

                PersonParentData personMotherData = data.getMother();
                if (personMotherData != null) {
                    motherPnr = personMotherData.getCprNumber();
                }

                PersonParentData personFatherData = data.getFather();
                if (personFatherData != null) {
                    fatherPnr = personFatherData.getCprNumber();
                }
            }
        }

        if (earliestProdDate != null) {
            item.put(OWN_PREFIX + PROD_DATE, earliestProdDate.format(dmyFormatter));
        }

        Filter parentFilter = new Filter(
                birthTime != null ?
                birthTime.atZone(StatisticsService.cprDataOffset).toOffsetDateTime() :
                earliestProdDate
        );
        item.put(MOTHER_PREFIX + PNR, motherPnr);
        if (motherPnr != null) {
            PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(motherPnr), PersonEntity.class);
            if (mother != null) {
                try {
                    item.putAll(this.formatParentPersonByRVD(mother, MOTHER_PREFIX, lookupService, parentFilter, true));
                } catch (Exclude e) {
                    // Do not include births where the mother lives outside of Greenland at the time of birth
                    return Collections.emptyMap();
                }
            }
        }

        item.put(FATHER_PREFIX + PNR, fatherPnr);
        if (fatherPnr != null) {
            PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(fatherPnr), PersonEntity.class);
            if (father != null) {
                try {
                    item.putAll(this.formatParentPersonByRVD(father, FATHER_PREFIX, lookupService, parentFilter, false));
                } catch (Exclude exclude) {
                    exclude.printStackTrace();
                }
            }
        }

        return item;
    }

    private Map<String, String> formatParentPersonByRecord(PersonEntity person, String prefix, LookupService lookupService, Filter filter, boolean excludeIfNonGreenlandic) throws Exclude {
        HashMap<String, String> item = new HashMap<>();
        for (AddressDataRecord addressDataRecord : sortRecords(filterRecordsByEffect(person.getAddress(), filter.effectAt))) {
            if (excludeIfNonGreenlandic && addressDataRecord.getMunicipalityCode() < 900) {
                throw new Exclude();
            }
            Lookup lookup = lookupService.doLookup(
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

            if (lookup.localityName != null) {
                item.put(prefix + LOCALITY_NAME, lookup.localityName);
            }
            if (lookup.localityAbbrev != null) {
                item.put(prefix + LOCALITY_ABBREVIATION, lookup.localityAbbrev);
            }
            if (lookup.localityCode != 0) {
                item.put(prefix + LOCALITY_CODE, Integer.toString(lookup.localityCode));
            }
        }

        for (CitizenshipDataRecord citizenshipDataRecord : sortRecords(filterRecordsByEffect(person.getCitizenship(), filter.effectAt))) {
            item.put(prefix + CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
        }

        for (BirthPlaceDataRecord birthPlaceDataRecord : sortRecords(filterRecordsByEffect(person.getBirthPlace(), filter.effectAt))) {
            item.put(prefix + BIRTH_AUTHORITY, Integer.toString(birthPlaceDataRecord.getAuthority()));
            item.put(prefix + BIRTH_AUTHORITY_TEXT, birthPlaceDataRecord.getBirthPlaceName());
        }
        return item;
    }

    private Map<String, String> formatParentPersonByRVD(PersonEntity person, String prefix, LookupService lookupService, Filter filter, boolean excludeIfNonGreenlandic) throws Exclude {

        HashMap<String, String> item = new HashMap<>();
        HashSet<PersonEffect> personEffects = new HashSet<>();
        for (PersonRegistration registration: person.getRegistrations()) {
            personEffects.addAll(registration.getEffectsAt(filter.effectAt));
        }

        ArrayList<PersonEffect> effects = new ArrayList<>(personEffects);
        effects.sort(Comparator.nullsFirst(this.personComparator));

        if (excludeIfNonGreenlandic) {
            for (PersonEffect effect: effects) {
                for (PersonBaseData data : effect.getDataItems()) {
                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        if (addressData.getMunicipalityCode() < 900) {
                            throw new Exclude();
                        }
                    }
                }
            }
        }

        for (PersonEffect effect: effects) {
            for (PersonBaseData data: effect.getDataItems()) {

                PersonAddressData addressData = data.getAddress();
                if (addressData != null) {
                    Lookup lookup = lookupService.doLookup(
                            addressData.getMunicipalityCode(),
                            addressData.getRoadCode(),
                            addressData.getHouseNumber()
                    );

                    item.put(prefix + MUNICIPALITY_CODE, Integer.toString(addressData.getMunicipalityCode()));
                    item.put(prefix + ROAD_CODE, formatRoadCode(addressData.getRoadCode()));
                    item.put(prefix + HOUSE_NUMBER, formatHouseNnr(addressData.getHouseNumber()));
                    item.put(prefix + FLOOR_NUMBER, addressData.getFloor());
                    item.put(prefix + DOOR_NUMBER, addressData.getDoor());
                    item.put(prefix + BNR, formatBnr(addressData.getBuildingNumber()));

                    if (lookup.localityName != null) {
                        item.put(prefix + LOCALITY_NAME, lookup.localityName);
                    }
                    if (lookup.localityAbbrev != null) {
                        item.put(prefix + LOCALITY_CODE, lookup.localityAbbrev);
                    }
                }

                PersonCitizenshipData citizenshipData = data.getCitizenship();
                if (citizenshipData != null) {
                    item.put(prefix + CITIZENSHIP_CODE, Integer.toString(citizenshipData.getCountryCode()));
                }

                PersonBirthData birthData = data.getBirth();
                if (birthData != null) {
                    if (birthData.getBirthPlaceCode() != null) {
                        item.put(prefix + BIRTH_AUTHORITY, Integer.toString(birthData.getBirthPlaceCode()));
                    }
                    if (birthData.getBirthAuthorityText() != null) {
                        item.put(prefix + BIRTH_AUTHORITY_TEXT, Integer.toString(birthData.getBirthAuthorityText()));
                    }
                }
            }
        }

        return item;
    }
}
