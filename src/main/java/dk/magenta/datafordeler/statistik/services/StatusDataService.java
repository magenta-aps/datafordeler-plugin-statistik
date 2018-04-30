package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.ListHashMap;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
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

    private Logger log = LoggerFactory.getLogger(BirthDataService.class);

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response, ServiceName.STATUS);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, FIRST_NAME, LAST_NAME, STATUS_CODE,
                BIRTH_AUTHORITY, CITIZENSHIP_CODE, MOTHER_PNR, FATHER_PNR, CIVIL_STATUS, SPOUSE_PNR,
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
    protected PersonQuery getQuery(HttpServletRequest request) {
        PersonStatusQuery personStatusQuery = new PersonStatusQuery();
        OffsetDateTime livingInGreenlandOnDate = Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER));
        if (livingInGreenlandOnDate != null) {
            personStatusQuery.setLivingInGreenlandOn(livingInGreenlandOnDate);
        }
        String pnr = request.getParameter("pnr");
        if (pnr != null) {
            personStatusQuery.setPersonnummer(pnr);
        }
        return personStatusQuery;
    }

    @Override
    protected Map<String, String> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());

        // Map of effectTime to addresses (when address was moved into)
        ListHashMap<OffsetDateTime, PersonAddressData> addresses = new ListHashMap<>();
        // Map of effectTime to registrationTime (when this move was first registered)
        HashMap<OffsetDateTime, OffsetDateTime> registrations = new HashMap<>();

        for (PersonRegistration registration : person.getRegistrations()) {
            for (PersonEffect effect : registration.getEffects()) {
                OffsetDateTime effectTime = effect.getEffectFrom();
                for (PersonBaseData data : effect.getDataItems()) {
                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        addresses.add(effectTime, addressData);

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

        OffsetDateTime latestCivilStatusDate = null;
        OffsetDateTime civilStatusRegistrationDate = null;
        OffsetDateTime latestAddressTime = null;
        PersonAddressData latestAddress = null;

        for (PersonRegistration registration : person.getRegistrations()){
            for (PersonEffect effect : registration.getEffectsAt(filter.effectAt)) {
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
            for (OffsetDateTime addressTime : addressTimes) {
                if (addresses.get(addressTime).contains(latestAddress)) {
                    item.put(MOVING_IN_DATE, addressTime.format(dmyFormatter));
                    item.put(MOVE_PROD_DATE, registrations.get(addressTime) != null ? registrations.get(addressTime).format(dmyFormatter) : null);
                    break;
                }
            }
        }
        return item;
    }
}
