package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.core.JsonProcessingException;
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


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void getMovement(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response, ServiceName.MOVEMENT);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, EFFECTIVE_PNR, STATUS_CODE, BIRTH_AUTHORITY, CITIZENSHIP_CODE,
                MOTHER_PNR, FATHER_PNR, SPOUSE_PNR, PROD_DATE, MOVE_DATE,
                ORIGIN_COUNTRY_CODE, ORIGIN_MUNICIPALITY_CODE, ORIGIN_LOCALITY_NAME, ORIGIN_ROAD_CODE, ORIGIN_HOUSE_NUMBER, ORIGIN_FLOOR, ORIGIN_DOOR_NUMBER, ORIGIN_BNR,
                DESTINATION_COUNTRY_CODE, DESTINATION_MUNICIPALITY_CODE, DESTINATION_LOCALITY_NAME, DESTINATION_ROAD_CODE, DESTINATION_HOUSE_NUMBER, DESTINATION_FLOOR, DESTINATION_DOOR_NUMBER, DESTINATION_BNR
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
    protected PersonQuery getQuery(HttpServletRequest request) {
        return new PersonMoveQuery(request);
    }

    @Override
    public List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter){
        // Map of effectTime to addresses (when address was moved into)
        HashMap<OffsetDateTime, AuthorityDetailData> addresses = new HashMap<>();
        // Map of effectTime to registrationTime (when this move was first registered)
        HashMap<OffsetDateTime, OffsetDateTime> registrations = new HashMap<>();

        for (PersonRegistration registration: person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffects()) {
                if (effect.getEffectFrom() != null && Objects.equals(effect.getEffectFrom(), effect.getEffectTo())) {
                    continue;  // Ignore effects with zero length
                }
                OffsetDateTime effectTime = effect.getEffectFrom();
                for (PersonBaseData data : effect.getDataItems()) {
                    boolean found = false;
                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        addresses.put(effectTime, addressData);
                        found = true;
                    }
                    /*
                    PersonForeignAddressData foreignAddressData = data.getForeignAddress();
                    if (foreignAddressData != null) {
                        addresses.put(effectTime, foreignAddressData);
                        found = true;
                    }
                    */
                    PersonEmigrationData emigrationData = data.getMigration();
                    if (emigrationData != null) {
                        addresses.put(effectTime, emigrationData);
                        found = true;
                    }
                    if (found) {
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
        HashMap<OffsetDateTime, Map<String, String>> moves = new HashMap<>();
        addressTimes.sort(Comparator.nullsFirst(OffsetDateTime::compareTo));

        int last = addressTimes.size() - 1;
        for (int i=0; i<=last; i++) {
            OffsetDateTime previous = i > 0 ? addressTimes.get(i-1) : null;
            OffsetDateTime current = addressTimes.get(i);
            OffsetDateTime currentRegistrationTime = registrations.get(current);

            if (current != null && currentRegistrationTime != null && (
                    (filter.after == null || current.isAfter(filter.after)) &&
                    (filter.before == null || current.isBefore(filter.before)) &&
                    (filter.registrationAfter == null || !currentRegistrationTime.isBefore(filter.registrationAfter))
            )) {
                AuthorityDetailData currentAddress = addresses.get(current);
                AuthorityDetailData previousAddress = addresses.get(previous);

                if (previousAddress == null || !isInGreenland(previousAddress) && !isInGreenland(currentAddress)) {
                    continue;
                }

                HashMap<String, String> item = new HashMap<>();
                item.put(PNR, person.getPersonnummer());

                if (previousAddress != null) {

                    if (previousAddress instanceof PersonAddressData) {
                        PersonAddressData previousDomesticAddress = (PersonAddressData) previousAddress;
                        item.put(ORIGIN_MUNICIPALITY_CODE, Integer.toString(previousDomesticAddress.getMunicipalityCode()));
                        item.put(ORIGIN_ROAD_CODE, formatRoadCode(previousDomesticAddress.getRoadCode()));
                        item.put(ORIGIN_HOUSE_NUMBER, formatHouseNnr(previousDomesticAddress.getHouseNumber()));
                        item.put(ORIGIN_FLOOR, previousDomesticAddress.getFloor());
                        item.put(ORIGIN_DOOR_NUMBER, previousDomesticAddress.getDoor());
                        item.put(ORIGIN_BNR, formatBnr(previousDomesticAddress.getBuildingNumber()));
                        Lookup lookup = lookupService.doLookup(previousDomesticAddress.getMunicipalityCode(), previousDomesticAddress.getRoadCode());
                        item.put(ORIGIN_LOCALITY_NAME, lookup.localityAbbrev);
                    }
                    /*if (previousAddress instanceof PersonForeignAddressData) {
                        PersonForeignAddressData previousForeignAddress = (PersonForeignAddressData) previousAddress;
                        try {
                            System.out.println(objectMapper.writerWithDefaultPrettyPrinter().writeValueAsString(previousForeignAddress));
                        } catch (JsonProcessingException e) {
                            e.printStackTrace();
                        }
                        item.put(ORIGIN_COUNTRY_CODE, Integer.toString(previousForeignAddress.getAuthority()));
                    }*/
                    if (previousAddress instanceof PersonEmigrationData) {
                        PersonEmigrationData previousMigration = (PersonEmigrationData) previousAddress;
                        item.put(ORIGIN_COUNTRY_CODE, Integer.toString(previousMigration.getCountryCode()));
                    }
                }
                if (currentAddress != null) {
                    if (currentAddress instanceof PersonAddressData) {
                        PersonAddressData currentDomesticAddress = (PersonAddressData) currentAddress;
                        item.put(DESTINATION_MUNICIPALITY_CODE, Integer.toString(currentDomesticAddress.getMunicipalityCode()));
                        item.put(DESTINATION_ROAD_CODE, formatRoadCode(currentDomesticAddress.getRoadCode()));
                        item.put(DESTINATION_HOUSE_NUMBER, formatHouseNnr(currentDomesticAddress.getHouseNumber()));
                        item.put(DESTINATION_FLOOR, currentDomesticAddress.getFloor());
                        item.put(DESTINATION_DOOR_NUMBER, currentDomesticAddress.getDoor());
                        item.put(DESTINATION_BNR, formatBnr(currentDomesticAddress.getBuildingNumber()));
                        item.put(MOVE_DATE, current.format(dmyFormatter));
                        if (registrations.containsKey(current)) {
                            item.put(PROD_DATE, registrations.get(current).format(dmyFormatter));
                        }
                        Lookup lookup = lookupService.doLookup(currentDomesticAddress.getMunicipalityCode(), currentDomesticAddress.getRoadCode());
                        item.put(DESTINATION_LOCALITY_NAME, lookup.localityAbbrev);
                    }
                    /*if (currentAddress instanceof PersonForeignAddressData) {
                        PersonForeignAddressData currentForeignAddress = (PersonForeignAddressData) currentAddress;
                        item.put(DESTINATION_COUNTRY_CODE, Integer.toString(currentForeignAddress.getAuthority()));
                        item.put(MOVE_DATE, current.format(dmyFormatter));
                        if (registrations.containsKey(current)) {
                            item.put(PROD_DATE, registrations.get(current).format(dmyFormatter));
                        }
                    }*/
                    if (currentAddress instanceof PersonEmigrationData) {
                        PersonEmigrationData currentMigration = (PersonEmigrationData) currentAddress;
                        item.put(DESTINATION_COUNTRY_CODE, Integer.toString(currentMigration.getCountryCode()));
                        item.put(MOVE_DATE, current.format(dmyFormatter));
                        if (registrations.containsKey(current)) {
                            item.put(PROD_DATE, registrations.get(current).format(dmyFormatter));
                        }
                    }
                }
                moves.put(current, item);
            }
        }


        if (moves.isEmpty()) {
            return Collections.emptyList();
        }


        for (PersonRegistration registration: person.getRegistrations()){
            for (OffsetDateTime moveTime : moves.keySet()) {
                Map<String, String> item = moves.get(moveTime);
                // Important: Populate the appropriate map with data as is relevant at the time of moving
                for (PersonEffect effect : registration.getEffectsAt(moveTime)) {
                    for (PersonBaseData data : effect.getDataItems()) {

                        //Check the type of service here and define with constructor to use for that service.
                        //There most be an integer or any other kind of flag for the service.
                        //   it can be a simple if checking of an integer

                        PersonBirthData birthData = data.getBirth();
                        if (birthData != null) {
                            if (birthData.getBirthDatetime() != null) {
                                item.put(BIRTHDAY_YEAR, Integer.toString(birthData.getBirthDatetime().getYear()));
                            }
                            if (birthData.getBirthPlaceCode() != null) {
                                item.put(BIRTH_AUTHORITY, Integer.toString(birthData.getBirthPlaceCode()));
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
        }
        return new ArrayList<>(moves.values());
    }

    private static boolean isInGreenland(AuthorityDetailData addressData) {
        boolean glFound = false;
        if (addressData != null) {
            if (addressData instanceof PersonAddressData) {
                PersonAddressData previousDomesticAddress = (PersonAddressData) addressData;
                if (previousDomesticAddress.getMunicipalityCode() > 900) {
                    glFound = true;
                }
            }
        }
        return glFound;
    }
}