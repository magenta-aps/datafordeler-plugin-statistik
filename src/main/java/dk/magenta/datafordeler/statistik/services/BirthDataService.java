package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.AccessDeniedException;
import dk.magenta.datafordeler.core.exception.AccessRequiredException;
import dk.magenta.datafordeler.core.exception.InvalidTokenException;
import dk.magenta.datafordeler.core.exception.MissingParameterException;
import dk.magenta.datafordeler.core.fapi.Query;
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



    private Logger log = LoggerFactory.getLogger(BirthDataService.class);


    //This function should have the following inputs:
    // birth year
    // registration before date

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void getBirth(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException {
        super.get(request, response);
    }


    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, EFFECTIVE_PNR, STATUS_CODE, BIRTH_AUTHORITY,
                MOTHER_PNR, MOTHER_BIRTH_AUTHORIRTY, MOTHER_STATUS, MOTHER_MUNICIPALITY_CODE, MOTHER_LOCALITY_NAME, MOTHER_ROAD_CODE, MOTHER_HOUSE_NUMBER, MOTHER_DOOR_NUMBER, MOTHER_BNR,
                FATHER_PNR, FATHER_BIRTH_AUTHORIRTY, FATHER_STATUS, FATHER_MUNICIPALITY_CODE, FATHER_LOCALITY_NAME, FATHER_ROAD_CODE, FATHER_HOUSE_NUMBER, FATHER_DOOR_NUMBER, FATHER_BNR
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
        return personBirthQuery;
    }

    @Override
    protected Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter) {
        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());
        item.put("effective_pnr", person.getPersonnummer());

        LookupService lookupService = new LookupService(session);

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {


                    PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }

                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthDatetime() != null) {
                            item.put("birth_year", birthData.getBirthDatetime().getYear());
                        }
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put("birth_authority", birthData.getBirthPlaceCode());
                        }
                    }

                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put("status_code", statusData.getStatus());
                    }

                    //TODO: own prod date (to be investigated)

                    PersonParentData personMotherData = data.getMother();
                    if (personMotherData != null) {


                        item.put("mother_pnr", personMotherData.getCprNumber());

                        PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(personMotherData.getCprNumber()), PersonEntity.class);
                        if (mother != null) {
                            item.putAll(this.formatParentPerson(mother, session, "mother_", lookupService));
                        }
                    }

                    PersonParentData personFatherData = data.getFather();
                    if (personFatherData != null) {
                        item.put("father_pnr", personFatherData.getCprNumber());
                        PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(personFatherData.getCprNumber()), PersonEntity.class);
                        if (father != null) {
                            item.putAll(this.formatParentPerson(father, session, "father_", lookupService));
                        }
                    }
                }
            }
        }
        return item;
    }

    @Override
    protected Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix, LookupService lookupService) {

        HashMap<String, Object> item = new HashMap<String, Object>();

        System.out.println("Lookup Object reference"+ lookupService.toString());

        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect: registration.getEffects()) {
                for (PersonBaseData data: effect.getDataItems()) {
                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null){
                        item.put(prefix + "status", statusData.getStatus());
                    }

                    PersonAddressData addressData = data.getAddress();
                    if(addressData != null){
                        Lookup lookup = lookupService.doLookup(addressData.getMunicipalityCode(), addressData.getRoadCode());

                        item.put(prefix + "municipality_code", addressData.getMunicipalityCode() );
                        item.put(prefix + "road_code", addressData.getRoadCode());
                        item.put(prefix + "house_number", addressData.getHouseNumber());
                        item.put(prefix + "door_number", addressData.getDoor());
                        item.put(prefix + "bnr", addressData.getBuildingNumber());

                        if (lookup.localityName != null) {
                            item.put(prefix + "locality_name", lookup.localityName);
                        }
                    }

                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(prefix + "birth_authority", birthData.getBirthPlaceCode());
                        }
                    }
                }
            }
        }
        return item;
    }
}
