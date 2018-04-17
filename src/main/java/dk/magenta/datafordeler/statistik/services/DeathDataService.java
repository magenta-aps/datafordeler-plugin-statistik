package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cpr.data.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonDeathQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.Lookup;
import dk.magenta.datafordeler.statistik.utils.LookupService;
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
import java.time.format.DateTimeFormatter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/*Created by Efrin 06-04-2018*/

@RestController
@RequestMapping("/statistik/death_data")
public class DeathDataService extends StatisticsService {
    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private CprPlugin cprPlugin;

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);

    @RequestMapping(method = RequestMethod.GET, path = "/", produces = {MediaType.TEXT_PLAIN_VALUE})
    public void getDeath(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                "status_code", "death_date", "prod_date", "pnr", "birth_year",
                "mother_pnr", "father_pnr", "spouse_pnr",
                "effective_pnr", "birth_authority", "municipality_code",
                "locality_name", "locality_code","road_code", "house_number", "door_number", "bnr"
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
        PersonDeathQuery personDeathQuery = new PersonDeathQuery();

        OffsetDateTime diedBeforeDate = Query.parseDateTime(request.getParameter(BEFORE_DATE_PARAMETER));
        if (diedBeforeDate != null) {
            personDeathQuery.setDeathDateTimeBefore(diedBeforeDate.toLocalDateTime()); // Timezone?
        }

        OffsetDateTime diedAfterDate = Query.parseDateTime(request.getParameter(AFTER_DATE_PARAMETER));
        if (diedAfterDate != null) {
            personDeathQuery.setDeathDateTimeAfter(diedAfterDate.toLocalDateTime()); // Timezone?
        }

        return personDeathQuery;
    }

    protected Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter) {
        HashMap<String, Object> item = new HashMap<String, Object>();
        item.put("pnr", person.getPersonnummer());
        item.put("effective_pnr", person.getPersonnummer());

        LookupService lookupService = new LookupService(session);
        OffsetDateTime earliestProdDate = null;

        OffsetDateTime earliestDeathTime = null;

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
                        if (birthData.getBirthAuthorityText() != null) {
                            item.put("birth_authority", birthData.getBirthAuthorityText());
                        }
                    }

                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put("status_code", statusData.getStatus());
                        if (statusData.getStatus() == 90) {
                            if (effect.getEffectFrom() != null && (earliestDeathTime == null || effect.getEffectFrom().isBefore(earliestDeathTime))) {
                                earliestDeathTime = effect.getEffectFrom();
                            }
                            if (registration.getRegistrationFrom() != null && (earliestProdDate == null || registration.getRegistrationFrom().isBefore(earliestProdDate))) {
                                earliestProdDate = registration.getRegistrationFrom();
                            }
                        }
                    }


                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        item.put("road_code", addressData.getRoadCode());
                        item.put("house_number", addressData.getHouseNumber());
                        item.put("door_number", addressData.getDoor());
                        item.put("bnr", addressData.getBuildingNumber());
                        item.put("municipality_code", addressData.getMunicipalityCode());
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

                    PersonCivilStatusData personCivilStatusData = data.getCivilStatus();
                    if (personCivilStatusData != null) {
                        item.put("spouse_pnr", personCivilStatusData.getSpouseCpr());
                    }
                    
                }
            }
        }
        if (earliestDeathTime != null) {
            item.put("death_date", earliestDeathTime.toLocalDate().format(DateTimeFormatter.ISO_LOCAL_DATE)); // Timezone?
        }
        if (earliestProdDate != null) {
            item.put("prod_date", earliestProdDate.format(dmyFormatter));
        }
        return item;
    }

    @Override
    protected Map<String, Object> formatParentPerson(PersonEntity person, Session session, String prefix) {
        return null;
    }

}