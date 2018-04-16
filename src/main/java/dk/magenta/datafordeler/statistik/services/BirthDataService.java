package dk.magenta.datafordeler.statistik.services;

/*Birth data service Extract the following for a person:
    own pnr
    own birth year
    own effective pnr
    own birth authority code (data missing, import handled in another ticket)
    own status code
    own prod date (to be investigated)

    mother's pnr
    mother's birth authority code (data missing, import handled in another ticket)
    mother's status code
    mother's municipality code
    mother's locality name
    mother's road code
    mother's house number
    mother's door/apartment no.
    mother's bnr

    father's pnr
    father's birth authority code (data missing, import handled in another ticket)
    father's status code
    father's municipality code
    father's locality name
    father's road code
    father's house number
    father's door/apartment no.
    father's bnr

Input parameters:
    birth year
    registration before date
    */

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
import java.util.*;
import java.util.stream.Stream;


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
        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER)));

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();







        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            this.writeItems(this.formatItems(personEntities, secondary_session, filter), response);
        } finally {
            primary_session.close();
            secondary_session.close();
        }
    }


    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "pnr", "birth_year", "effective_pnr", "status_code",
                "mother_pnr", "mother_status", "mother_municipality_code", "mother_road_code", "mother_house_number", "mother_door_number", "mother_bnr",
                "father_pnr", "father_status", "father_municipality_code", "father_road_code", "father_house_number", "father_door_number", "father_bnr",
                "locality_name", "road_code", "house_number", "door_number", "bnr","municipality_code"
        });

    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }

    @Override
    protected Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter) {
        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonBirthData birthData = data.getBirth();
                        if (birthData != null) {
                            if (birthData.getBirthDatetime() != null) {
                                item.put("birth_year", birthData.getBirthDatetime().getYear());
                            }
                        }
                    PersonStatusData statusData = data.getStatus();
                        if (statusData != null) {
                            item.put("status_code", statusData.getStatus());
                        }

                    item.put("effective_pnr", person.getPersonnummer());

                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {

                        //TODO: Locatility need to be here. Not sure what it means.
                        item.put("road_code", addressData.getRoadCode());
                        item.put("house_number", addressData.getHouseNumber());
                        item.put("door_number", addressData.getDoor());
                        item.put("bnr", addressData.getBuildingNumber());
                        item.put("municipality_code", addressData.getMunicipalityCode());

                    }

                    PersonParentData personMotherData = data.getMother();
                        if (personMotherData != null) {
                            item.put("mother_pnr", personMotherData.getCprNumber());
                            PersonEntity mother = QueryManager.getEntity(session, PersonEntity.generateUUID(personMotherData.getCprNumber()), PersonEntity.class);
                            if (mother != null) {
                                item.putAll(this.formatParentPerson(mother, session, "mother_"));
                            }
                        }

                    PersonParentData personFatherData = data.getFather();
                        if (personFatherData != null) {
                            item.put("father_pnr", personFatherData.getCprNumber());
                            PersonEntity father = QueryManager.getEntity(session, PersonEntity.generateUUID(personFatherData.getCprNumber()), PersonEntity.class);
                            if (father != null) {
                                item.putAll(this.formatParentPerson(father, session, "father_"));
                            }
                    }



                }
            }
        }
        return item;

    }

    @Override
    protected Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix) {


        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put(prefix + "pnr", person.getPersonnummer());
        LookupService lookupService = new LookupService(session);

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
                            item.put(prefix + "locality", lookup.localityName);
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

    @Override
    protected Iterator<Map<String, Object>> formatItems(Stream<PersonEntity> personEntities, Session secondary_session, Filter filter) {
        return personEntities.map(personEntity -> formatPerson(personEntity, secondary_session, filter)).iterator();

    }
}
