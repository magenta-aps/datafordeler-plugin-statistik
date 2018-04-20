package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
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
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


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

    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response, ServiceName.STATUS);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                PNR, BIRTHDAY_YEAR, FIRST_NAME, LAST_NAME, STATUS_CODE,
                BIRTH_AUTHORITY, MOTHER_PNR, FATHER_PNR, SPOUSE_PNR, CIVIL_STATUS,
                MUNICIPALITY_CODE, LOCALITY_NAME, LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, DOOR_NUMBER, FLOOR_NUMBER,
                BNR, MOVING_IN_DATE, POST_CODE, CIVIL_STATUS_DATE, CHURCH

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
        PersonStatusQuery personStatusQuery = new PersonStatusQuery();
        OffsetDateTime livingInGreenlandOnDate = Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER));
        if (livingInGreenlandOnDate != null) {
            personStatusQuery.setLivingInGreenlandOn(livingInGreenlandOnDate);
        }
        return personStatusQuery;
    }

    @Override
    protected Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter) {
        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put(PNR, person.getPersonnummer());
        LookupService lookupService = new LookupService(session);

        OffsetDateTime latestCivilStatusDate = null;

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonNameData nameData = data.getName();
                    if (nameData != null) {
                        item.put(FIRST_NAME, nameData.getFirstNames());
                        item.put(LAST_NAME, nameData.getLastName());
                    }

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
                        item.put(STATUS_CODE, statusData.getStatus());
                    }

                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        item.put(POST_CODE, addressData.getPostalCode());
                        item.put(MUNICIPALITY_CODE, addressData.getMunicipalityCode());
                        item.put(ROAD_CODE, addressData.getRoadCode());
                        item.put(HOUSE_NUMBER, addressData.getHouseNumber());
                        item.put(DOOR_NUMBER, addressData.getDoor());
                        item.put(BNR, addressData.getBuildingNumber());
                        item.put(FLOOR_NUMBER,addressData.getFloor());
                        Lookup lookup = lookupService.doLookup(addressData.getMunicipalityCode(), addressData.getRoadCode());
                        if (lookup != null) {
                            item.put(LOCALITY_NAME, lookup.localityName);
                            item.put(LOCALITY_CODE, lookup.localityCode);
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
                    if (personCivilStatus != null ){
                        item.put(CIVIL_STATUS, personCivilStatus.getCivilStatus());
                        if (effect.getEffectFrom() != null && (latestCivilStatusDate == null || effect.getEffectFrom().isAfter(latestCivilStatusDate))) {
                            latestCivilStatusDate = effect.getEffectFrom();
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
        return item;
    }
}
