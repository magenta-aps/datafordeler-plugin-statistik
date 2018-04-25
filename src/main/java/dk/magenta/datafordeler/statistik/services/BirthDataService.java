package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.OffsetDateTimeAdapter;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
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
import java.time.ZoneOffset;
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

    private Logger log = LoggerFactory.getLogger(BirthDataService.class);


    //This function should have the following inputs:
    // birth year
    // registration before date

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        super.get(request, response, ServiceName.BIRTH);
    }

    private static final String OWN_PREFIX = "B_";

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                OWN_PREFIX + PNR, OWN_PREFIX + BIRTHDAY_YEAR, OWN_PREFIX + EFFECTIVE_PNR, OWN_PREFIX + BIRTH_AUTHORITY, OWN_PREFIX + CITIZENSHIP_CODE, OWN_PREFIX + PROD_DATE,
                MOTHER_PREFIX + PNR, MOTHER_PREFIX + BIRTH_AUTHORITY, MOTHER_PREFIX + CITIZENSHIP_CODE, MOTHER_PREFIX + MUNICIPALITY_CODE, MOTHER_PREFIX + LOCALITY_NAME, MOTHER_PREFIX + LOCALITY_CODE, MOTHER_PREFIX + ROAD_CODE, MOTHER_PREFIX + HOUSE_NUMBER, MOTHER_PREFIX + FLOOR_NUMBER, MOTHER_PREFIX + DOOR_NUMBER, MOTHER_PREFIX + BNR,
                FATHER_PREFIX + PNR, FATHER_PREFIX + BIRTH_AUTHORITY, FATHER_PREFIX + CITIZENSHIP_CODE, FATHER_PREFIX + MUNICIPALITY_CODE, FATHER_PREFIX + LOCALITY_NAME, FATHER_PREFIX + LOCALITY_CODE, FATHER_PREFIX + ROAD_CODE, FATHER_PREFIX + HOUSE_NUMBER, FATHER_PREFIX + FLOOR_NUMBER, FATHER_PREFIX + DOOR_NUMBER, FATHER_PREFIX + BNR
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
    protected PersonQuery getQuery(HttpServletRequest request) {
        PersonBirthQuery personBirthQuery = new PersonBirthQuery();
        OffsetDateTime bornBeforeDate = Query.parseDateTime(request.getParameter(BEFORE_DATE_PARAMETER));
        if (bornBeforeDate != null) {
            personBirthQuery.setBirthDateTimeBefore(bornBeforeDate.toLocalDateTime()); // Timezone?
        }
        OffsetDateTime bornAfterDate = Query.parseDateTime(request.getParameter(AFTER_DATE_PARAMETER));
        if (bornAfterDate != null) {
            personBirthQuery.setBirthDateTimeAfter(bornAfterDate.toLocalDateTime()); // Timezone?
        }
        String pnr = request.getParameter("pnr");
        if (pnr != null) {
            personBirthQuery.setPersonnummer(pnr);
        }
        personBirthQuery.setPageSize(1000000);
        return personBirthQuery;
    }

    @Override
    protected Map<String, String> formatPerson(PersonEntity person, Session session, Filter filter) {
        HashMap<String, String> item = new HashMap<String, String>();
        item.put(OWN_PREFIX + PNR, person.getPersonnummer());
        //item.put(OWN_PREFIX + EFFECTIVE_PNR, person.getPersonnummer());

        LookupService lookupService = new LookupService(session);
        OffsetDateTime earliestProdDate = null;
        LocalDateTime birthTime = null;
        String motherPnr = null;
        String fatherPnr = null;

        HashSet<PersonEffect> personEffects = new HashSet<>();
        for (PersonRegistration registration: person.getRegistrations()) {
            personEffects.addAll(registration.getEffectsAt(filter.effectAt));
        }

        ArrayList<PersonEffect> effects = new ArrayList<>(personEffects);
        effects.sort(Comparator.nullsFirst(PersonEffect::compareTo));

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
                    OffsetDateTime registrationFrom = effect.getRegistration().getRegistrationFrom();
                    if (registrationFrom != null && (earliestProdDate == null || registrationFrom.isBefore(earliestProdDate))) {
                        earliestProdDate = registrationFrom;
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

        Filter parentFilter = new Filter(birthTime.atOffset(ZoneOffset.UTC));
        item.put(MOTHER_PREFIX + PNR, motherPnr);
        if (motherPnr != null) {
            PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(motherPnr), PersonEntity.class);
            if (mother != null) {
                try {
                    item.putAll(this.formatParentPerson(mother, MOTHER_PREFIX, lookupService, parentFilter, true));
                } catch (Exclude e) {
                    // Do not include births where the mother lives outside of Greenland at the time of birth
                    return null;
                }
            }
        }


        item.put(FATHER_PREFIX + PNR, fatherPnr);
        if (fatherPnr != null) {
            PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(fatherPnr), PersonEntity.class);
            if (father != null) {
                try {
                    item.putAll(this.formatParentPerson(father, FATHER_PREFIX, lookupService, parentFilter, false));
                } catch (Exclude exclude) {
                    exclude.printStackTrace();
                }
            }
        }

        return item;
    }

    private Map<String, String> formatParentPerson(PersonEntity person, String prefix, LookupService lookupService, Filter filter, boolean excludeIfNonGreenlandic) throws Exclude {
        HashMap<String, String> item = new HashMap<String, String>();
        HashSet<PersonEffect> personEffects = new HashSet<>();
        for (PersonRegistration registration: person.getRegistrations()) {
            personEffects.addAll(registration.getEffectsAt(filter.effectAt));
        }

        ArrayList<PersonEffect> effects = new ArrayList<>(personEffects);
        effects.sort(Comparator.nullsFirst(PersonEffect::compareTo));

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
                }
            }
        }

        return item;
    }
}
