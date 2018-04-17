package dk.magenta.datafordeler.statistik.services;
/*Extract the following for a person:
    pnr
    birth year
    firstname
    lastname
    status code
    birth municipality code (data missing, import handled in another ticket)
    mother's pnr
    father's pnr
    civil status
    spouse's pnr
    municipality code
    locality name
    road code
    house number
    floor
    door/apartment no.
    bnr
    moving-in date
    postcode
    civil status date
    church (to be investigated)

Input parameters:
    living in Greenland on date*/

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonBirthQuery;
import dk.magenta.datafordeler.statistik.queries.PersonStatusQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.stream.Stream;


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

    private Logger log = LoggerFactory.getLogger(BirthDataService.class);


    //This function should have the following inputs:
    //living in Greenland on date


    public static int[] glMunicipalityCodes = new int[]{955, 956, 957, 958, 961};

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void getStatus(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {

        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER)));

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();

        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            this.writeItems(this.formatItems(personEntities, secondary_session, filter), response);
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            primary_session.close();
            secondary_session.close();
        }
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "pnr", "birth_year", "first_name", "last_name", "status_code",
                "birth_authority", "mother_pnr","father_pnr", "spouse_pnr", "civil_status",
                "municipality_code", "locality_name", "road_code", "house_number", "door_number",
                "bnr", "moving_in_date", "post_code", "civil_status_date", "church"

        });
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }

    @Override
    protected PersonQuery getQuery(HttpServletRequest request) {
        PersonStatusQuery personStatusQuery = new PersonStatusQuery();
        OffsetDateTime livingInGreenlandOnDate = Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER));
        if (livingInGreenlandOnDate != null) {
            personStatusQuery.setLivingInGreenlandOn(livingInGreenlandOnDate);
        }

        return personStatusQuery;
    }

    @Override
    protected Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter) {
System.out.println("Format person");
        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    //Check the type of service here and define with constructor to use for that service.
                    //There most be an integer or any other kind of flag for the service.
                    //   it can be a simple if checking of an integer


                    PersonNameData nameData = data.getName();
                    if (nameData != null) {
                        item.put("first_name", nameData.getFirstNames());
                        item.put("last_name", nameData.getLastName());
                    }

                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put("birth_authority", birthData.getBirthPlaceCode());
                        }
                    }

                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put("status_code", statusData.getStatus());
                    }



                    //This part of the code is duplicated in the function formatParentPerson.
                    // Check it out how it can be generalized.
                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        //item.put("post_code", addressData.getPostalCode());

                        item.put("municipality_code", addressData.getMunicipalityCode());
                        //Locatility need to be here
                        item.put("road_code", addressData.getRoadCode());
                        item.put("house_number", addressData.getHouseNumber());
                        item.put("door_number", addressData.getDoor());
                        item.put("bnr", addressData.getBuildingNumber());


                    }


                    //Missing prod date (not sure about the meaning)


                    //Intended to full fill the own information in contrary to parents
                    PersonCoreData personData = data.getCoreData();
                    if (personData != null) {
                        PersonEntity own = QueryManager.getEntity(session, PersonEntity.generateUUID(personData.getCprNumber()), PersonEntity.class);
                        if (own != null) {
                            item.putAll(this.formatParentPerson(own, session, ""));
                        }
                    }

                    PersonParentData personMotherData = data.getMother();
                    if (personMotherData != null) {
                        item.put("mother_pnr", personMotherData.getCprNumber());
                    }

                    PersonParentData personFatherData = data.getFather();
                    if (personFatherData != null) {
                        item.put("father_pnr", personFatherData.getCprNumber());
                    }

                    PersonCivilStatusData personSpouseData = data.getCivilStatus();
                    if (personSpouseData != null) {
                        // "civil_status_date"?

                        item.put("spouse_pnr", personSpouseData.getSpouseCpr());
                    }

                    PersonChurchData personChurchData = data.getChurch();
                    if (personChurchData != null) {
                        item.put("church", personChurchData.getChurchRelation().toString());
                    }

                }
            }
        }
        return item;
    }

    @Override
    protected Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix) {
        return null;
    }
}
