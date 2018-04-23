package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserManager;
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
@RequestMapping("/statistik/movement_data")
public class MovementDataService extends StatisticsService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);

    //This function should have the following inputs:
    //movement date


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void getDeath(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, EFFECTIVE_PNR, STATUS_CODE, BIRTH_AUTHORITY, CITIZENSHIP_CODE,
                MOTHER_PNR, FATHER_PNR, SPOUSE_PNR, PROD_DATE, MOVE_DATE,
                ORIGIN_MUNICIPALITY_CODE, ORIGIN_LOCALITY_NAME, ORIGIN_ROAD_CODE, ORIGIN_HOUSE_NUMBER, ORIGIN_FLOOR, ORIGIN_DOOR_NUMBER, ORIGIN_BNR,
                DESTINATION_MUNICIPALITY_CODE, DESTINATION_LOCALITY_NAME, DESTINATION_ROAD_CODE, DESTINATION_HOUSE_NUMBER, DESTINATION_FLOOR, DESTINATION_DOOR_NUMBER, DESTINATION_BNR
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
        PersonMoveQuery personMoveQuery = new PersonMoveQuery();
        OffsetDateTime moveAfterDate = Query.parseDateTime(request.getParameter(AFTER_DATE_PARAMETER));
        if (moveAfterDate != null) {
            personMoveQuery.setMoveDateTimeAfter(moveAfterDate);
        }
        OffsetDateTime moveBeforeDate = Query.parseDateTime(request.getParameter(BEFORE_DATE_PARAMETER));
        if (moveBeforeDate != null) {
            personMoveQuery.setMoveDateTimeBefore(moveBeforeDate);
        }
        String pnr = request.getParameter("pnr");
        if (pnr != null) {
            personMoveQuery.setPersonnummer(pnr);
        }
        return personMoveQuery;
    }

    @Override
    public Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter){
        HashMap<String, Object> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());

        // Map of effectTime to addresses (when address was moved into)
        HashMap<OffsetDateTime, PersonAddressData> addresses = new HashMap<>();
        // Map of effectTime to registrationTime (when this move was first registered)
        HashMap<OffsetDateTime, OffsetDateTime> registrations = new HashMap<>();

        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffects()) {
                OffsetDateTime effectTime = effect.getEffectFrom();
                for (PersonBaseData data : effect.getDataItems()) {
                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        addresses.put(effectTime, addressData);

                        if (!registrations.containsKey(effectTime)) {
                            OffsetDateTime oldTime = registrations.get(effectTime);
                            OffsetDateTime newTime = registration.getRegistrationFrom();
                            if (newTime != null && (oldTime == null || newTime.isBefore(oldTime))) {
                                registrations.put(effectTime, newTime);
                            }
                        }
                    }
                }
            }
        }
        ArrayList<OffsetDateTime> addressTimes = new ArrayList<>(addresses.keySet());
        addressTimes.sort(Comparator.nullsFirst(OffsetDateTime::compareTo));

        int last = addressTimes.size() - 1;
        for (int i=0; i<=last; i++) {
            OffsetDateTime previous = i > 0 ? addressTimes.get(i-1) : null;
            OffsetDateTime current = addressTimes.get(i);
            OffsetDateTime next = i < last ? addressTimes.get(i+1) : null;

            if ((current == null || !current.isAfter(filter.effectAt)) && (next == null || next.isAfter(filter.effectAt))) {
                PersonAddressData currentAddress = addresses.get(current);
                PersonAddressData previousAddress = addresses.get(previous);
                if (previousAddress != null) {
                    item.put(ORIGIN_MUNICIPALITY_CODE, previousAddress.getMunicipalityCode());
                    //item.put("origin_locality_name", null);
                    item.put(ORIGIN_ROAD_CODE, formatRoadCode(previousAddress.getRoadCode()));
                    item.put(ORIGIN_HOUSE_NUMBER, formatHouseNnr(previousAddress.getHouseNumber()));
                    item.put(ORIGIN_FLOOR, previousAddress.getFloor());
                    item.put(ORIGIN_DOOR_NUMBER, previousAddress.getDoor());
                    item.put(ORIGIN_BNR, formatBnr(previousAddress.getBuildingNumber()));
                }
                if (currentAddress != null) {
                    item.put(DESTINATION_MUNICIPALITY_CODE, currentAddress.getMunicipalityCode());
                    //item.put("destination_locality_name", null);
                    item.put(DESTINATION_ROAD_CODE, formatRoadCode(currentAddress.getRoadCode()));
                    item.put(DESTINATION_HOUSE_NUMBER, formatHouseNnr(currentAddress.getHouseNumber()));
                    item.put(DESTINATION_FLOOR, currentAddress.getFloor());
                    item.put(DESTINATION_DOOR_NUMBER, currentAddress.getDoor());
                    item.put(DESTINATION_BNR, formatBnr(currentAddress.getBuildingNumber()));
                    item.put(MOVE_DATE, current.format(dmyFormatter));
                    if (registrations.containsKey(current)) {
                        item.put(PROD_DATE, registrations.get(current).format(dmyFormatter));
                    }
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
                            item.put(BIRTHDAY_YEAR, birthData.getBirthDatetime().getYear());
                        }
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(BIRTH_AUTHORITY, birthData.getBirthPlaceCode());
                        }
                    }


                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put(STATUS_CODE, formatStatusCode(statusData.getStatus()));
                    }

                    PersonCitizenshipData citizenshipData = data.getCitizenship();
                    if (citizenshipData != null) {
                        item.put(CITIZENSHIP_CODE, citizenshipData.getCountryCode());
                    }

                    PersonCoreData coreData = data.getCoreData();
                    if (coreData != null) {
                        item.put(EFFECTIVE_PNR, coreData.getCprNumber());
                    }

                    PersonParentData personMotherData = data.getMother();
                    if (personMotherData != null) {
                        item.put(MOTHER_PNR, personMotherData.getCprNumber());
                    }

                    PersonParentData personFatherData = data.getFather();
                    if (personFatherData != null) {
                        item.put(FATHER_PNR, personFatherData.getCprNumber());
                    }

                    PersonCivilStatusData personSpouseData = data.getCivilStatus();
                    if (personSpouseData != null) {
                        item.put(SPOUSE_PNR, personSpouseData.getSpouseCpr());
                    }
                }
            }
        }
        return item;
    }
}