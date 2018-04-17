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
    public void getStatus(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "pnr", "birth_year", "first_name", "last_name", "status_code",
                "birth_authority", "mother_pnr","father_pnr", "spouse_pnr", "civil_status",
                "municipality_code", "locality_name", "locality_code", "road_code", "house_number", "door_number", "floor_number",
                "bnr", "moving_in_date", "post_code", "civil_status_date", "church"

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
        item.put("pnr", person.getPersonnummer());
        LookupService lookupService = new LookupService(session);

        OffsetDateTime latestCivilStatusDate = null;

        for (PersonRegistration registration: person.getRegistrations()){
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonNameData nameData = data.getName();
                    if (nameData != null) {
                        item.put("first_name", nameData.getFirstNames());
                        item.put("last_name", nameData.getLastName());
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


                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        item.put("post_code", addressData.getPostalCode());
                        item.put("municipality_code", addressData.getMunicipalityCode());
                        item.put("road_code", addressData.getRoadCode());
                        item.put("house_number", addressData.getHouseNumber());
                        item.put("door_number", addressData.getDoor());
                        item.put("bnr", addressData.getBuildingNumber());
                        item.put("floor_number",addressData.getFloor());
                        Lookup lookup = lookupService.doLookup(addressData.getMunicipalityCode(), addressData.getRoadCode());
                        if (lookup != null) {
                            item.put("locality_name", lookup.localityName);
                            item.put("locality_code", lookup.localityCode);
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

                    PersonCivilStatusData personCivilStatus = data.getCivilStatus();
                    if (personCivilStatus != null ){
                        item.put("civil_status", personCivilStatus.getCivilStatus());
                        if (effect.getEffectFrom() != null && (latestCivilStatusDate == null || effect.getEffectFrom().isAfter(latestCivilStatusDate))) {
                            latestCivilStatusDate = effect.getEffectFrom();
                        }
                    }

                    PersonCivilStatusData personSpouseData = data.getCivilStatus();
                    if (personSpouseData != null) {
                        item.put("spouse_pnr", personSpouseData.getSpouseCpr());
                    }

                    PersonChurchData personChurchData = data.getChurch();
                    if (personChurchData != null) {
                        item.put("church", personChurchData.getChurchRelation().toString());
                    }

                }
            }
        }
        if (latestCivilStatusDate != null) {
            item.put("civil_status_date", latestCivilStatusDate.format(dmyFormatter));
        }
        return item;
    }

    @Override
    protected Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix) {
        return null;
    }
}
