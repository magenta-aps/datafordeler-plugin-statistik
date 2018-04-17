package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonMoveQuery;
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
@RequestMapping("/statistik/movement_data")
public class MovementDataService extends StatisticsService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private CprPlugin cprPlugin;

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);


    //This function should have the following inputs:
    //movement date

    @RequestMapping(method = RequestMethod.GET, path = "/", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getDeath(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {

        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));

        OffsetDateTime effectDate = Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(effectDate);

        final Session primary_session = sessionManager.getSessionFactory().openSession();
        final Session secondary_session = sessionManager.getSessionFactory().openSession();

        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            this.writeItems(this.formatItems(personEntities,secondary_session, filter), response);
        } finally {
            primary_session.close();
            secondary_session.close();
        }
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "pnr", "birth_year", "effective_pnr", "status_code", "birth_authority",
                "mother_pnr", "father_pnr", "spouse_pnr", "prod_date",
                "origin_municipality_code", "origin_locality_name", "origin_road_code", "origin_house_number", "origin_floor", "origin_door_number", "origin_bnr",
                "destination_municipality_code", "destination_locality_name", "destination_road_code", "destination_house_number", "destination_floor", "destination_door_number", "destination_bnr",
        });
    }

    @Override
    protected CsvMapper getCsvMapper() {
        return this.csvMapper;
    }

    @Override
    protected PersonQuery getQuery(HttpServletRequest request) {
        PersonMoveQuery personMoveQuery = new PersonMoveQuery();
        OffsetDateTime moveAfterDate = Query.parseDateTime(request.getParameter(AFTER_DATE_PARAMETER));
        if (moveAfterDate != null) {
            personMoveQuery.setMoveDateTimeAfter(moveAfterDate);
        }
        OffsetDateTime moveBeforeDate = Query.parseDateTime(request.getParameter(BEFORE_DATE_PARAMETER));
        if (moveBeforeDate != null) {
            personMoveQuery.setMoveDateTimeBefore(moveBeforeDate);
        }
        return personMoveQuery;
    }

    @Override
    public Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter){
        HashMap<String, Object> item = new HashMap<>();
        item.put("pnr", person.getPersonnummer());

        HashMap<OffsetDateTime, PersonAddressData> addresses = new HashMap<>();

        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffects()) {
                for (PersonBaseData data : effect.getDataItems()) {
                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        addresses.put(effect.getEffectFrom(), addressData);
                    }
                }
            }
        }
        ArrayList<OffsetDateTime> addressTimes = new ArrayList<>(addresses.keySet());
        addressTimes.sort(Comparator.nullsFirst(OffsetDateTime::compareTo));

        for (int i=0; i<addressTimes.size(); i++) {
            OffsetDateTime previous = i > 0 ? addressTimes.get(i-1) : null;
            OffsetDateTime current = addressTimes.get(i);
            OffsetDateTime next = (i < addressTimes.size() - 1) ? addressTimes.get(i+1) : null;

            if ((current == null || !current.isAfter(filter.effectAt)) && (next == null || next.isAfter(filter.effectAt))) {
                PersonAddressData currentAddress = addresses.get(current);
                PersonAddressData previousAddress = addresses.get(previous);
                if (previousAddress != null) {
                    item.put("origin_municipality_code", previousAddress.getMunicipalityCode());
                    //item.put("origin_locality_name", null);
                    item.put("origin_road_code", previousAddress.getRoadCode());
                    item.put("origin_house_number", previousAddress.getHouseNumber());
                    item.put("origin_floor", previousAddress.getFloor());
                    item.put("origin_door_number", previousAddress.getDoor());
                    item.put("origin_bnr", previousAddress.getBuildingNumber());
                }
                if (currentAddress != null) {
                    item.put("destination_municipality_code", previousAddress.getMunicipalityCode());
                    //item.put("destination_locality_name", null);
                    item.put("destination_road_code", previousAddress.getRoadCode());
                    item.put("destination_house_number", previousAddress.getHouseNumber());
                    item.put("destination_floor", previousAddress.getFloor());
                    item.put("destination_door_number", previousAddress.getDoor());
                    item.put("destination_bnr", previousAddress.getBuildingNumber());
                }
            }
        }


        for (PersonRegistration registration: person.getRegistrations()){

            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {


                    //Check the type of service here and define with constructor to use for that service.
                    //There most be an integer or any other kind of flag for the service.
                     //   it can be a simple if checking of an integer

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


                    item.put("effective_pnr", person.getPersonnummer());
                    PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put("effective_pnr", coreData.getCprNumber());
                    }



                    //Missing prod date (not sure about the meaning)


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
                        item.put("spouse_pnr", personSpouseData.getSpouseCpr());
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