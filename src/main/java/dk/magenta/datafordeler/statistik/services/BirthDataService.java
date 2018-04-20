package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserManager;
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
import java.time.OffsetDateTime;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/birth_data")
public class BirthDataService extends StatisticsService {

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
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {
        super.get(request, response);
    }


    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, EFFECTIVE_PNR, BIRTH_AUTHORITY, STATUS_CODE, PROD_DATE,
                MOTHER_PNR, MOTHER_BIRTH_AUTHORIRTY, MOTHER_STATUS_CODE, MOTHER_MUNICIPALITY_CODE, MOTHER_LOCALITY_NAME, MOTHER_LOCALITY_CODE, MOTHER_ROAD_CODE, MOTHER_HOUSE_NUMBER, MOTHER_DOOR_NUMBER, MOTHER_BNR,
                FATHER_PNR, FATHER_BIRTH_AUTHORIRTY, FATHER_STATUS_CODE, FATHER_MUNICIPALITY_CODE, FATHER_LOCALITY_NAME, FATHER_LOCALITY_CODE, FATHER_ROAD_CODE, FATHER_HOUSE_NUMBER, FATHER_DOOR_NUMBER, FATHER_BNR
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
        return personBirthQuery;
    }

    @Override
    protected Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter) {
        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put(PNR, person.getPersonnummer());
        item.put(EFFECTIVE_PNR, person.getPersonnummer());

        LookupService lookupService = new LookupService(session);
        OffsetDateTime earliestProdDate = null;

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put(EFFECTIVE_PNR, coreData.getCprNumber());
                    }

                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthDatetime() != null) {
                            item.put(BIRTHDAY_YEAR, birthData.getBirthDatetime().getYear());
                        }
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(BIRTH_AUTHORITY, birthData.getBirthPlaceCode());
                        }
                        if (registration.getRegistrationFrom() != null && (earliestProdDate == null || registration.getRegistrationFrom().isBefore(earliestProdDate))) {
                            earliestProdDate = registration.getRegistrationFrom();
                        }
                    }

                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put(STATUS_CODE, formatStatusCode(statusData.getStatus()));
                    }

                    PersonParentData personMotherData = data.getMother();
                    if (personMotherData != null) {
                        item.put(MOTHER_PNR, personMotherData.getCprNumber());
                        PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(personMotherData.getCprNumber()), PersonEntity.class);
                        if (mother != null) {
                            item.putAll(this.formatParentPerson(mother, session, MOTHER_PREFIX, lookupService));
                        }
                    }

                    PersonParentData personFatherData = data.getFather();
                    if (personFatherData != null) {
                        item.put(FATHER_PNR, personFatherData.getCprNumber());
                        PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(personFatherData.getCprNumber()), PersonEntity.class);
                        if (father != null) {
                            item.putAll(this.formatParentPerson(father, session, FATHER_PREFIX, lookupService));
                        }
                    }
                }
            }
        }
        if (earliestProdDate != null) {
            item.put(PROD_DATE, earliestProdDate.format(dmyFormatter));
        }
        return item;
    }

    private Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix, LookupService lookupService) {
        HashMap<String, Object> item = new HashMap<String, Object>();
        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect: registration.getEffects()) {
                for (PersonBaseData data: effect.getDataItems()) {
                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null){
                        item.put(prefix + "status_code", statusData.getStatus());
                    }

                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        Lookup lookup = lookupService.doLookup(
                                addressData.getMunicipalityCode(),
                                addressData.getRoadCode(),
                                addressData.getHouseNumber()
                        );


                        item.put(prefix + MUNICIPALITY_CODE, addressData.getMunicipalityCode() );
                        item.put(prefix + ROAD_CODE, formatRoadCode(addressData.getRoadCode()));
                        item.put(prefix + HOUSE_NUMBER, formatHouseNnr(addressData.getHouseNumber()));
                        item.put(prefix + DOOR_NUMBER, addressData.getDoor());
                        item.put(prefix + BNR, formatBnr(addressData.getBuildingNumber()));


                        if (lookup.localityName != null) {
                            item.put(prefix + LOCALITY_NAME, lookup.localityName);
                        }
                        if (lookup.localityAbbrev != null) {
                            item.put(prefix + LOCALITY_CODE, lookup.localityAbbrev);
                        }
                    }

                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(prefix + BIRTH_AUTHORITY, birthData.getBirthPlaceCode());
                        }
                    }
                }
            }
        }
        return item;
    }
}
