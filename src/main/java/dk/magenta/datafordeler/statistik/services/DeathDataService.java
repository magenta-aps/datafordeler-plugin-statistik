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
import dk.magenta.datafordeler.statistik.queries.PersonDeathQuery;
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
@RequestMapping("/statistik/death_data")
public class DeathDataService extends StatisticsService {
    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    DafoUserManager dafoUserManager;

    private Logger log = LoggerFactory.getLogger(DeathDataService.class);


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)

            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException {
        super.get(request, response, ServiceName.DEATH);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                STATUS_CODE , DEATH_DATE, PROD_DATE, PNR, BIRTHDAY_YEAR,
                MOTHER_PNR, FATHER_PNR, SPOUSE_PNR,
                EFFECTIVE_PNR, CITIZENSHIP_CODE, BIRTH_AUTHORITY, BIRTH_AUTHORITY_TEXT, MUNICIPALITY_CODE,
                LOCALITY_NAME, LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, DOOR_NUMBER, BNR
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
        PersonDeathQuery personDeathQuery = new PersonDeathQuery();

        OffsetDateTime diedBeforeDate = Query.parseDateTime(request.getParameter(BEFORE_DATE_PARAMETER));
        if (diedBeforeDate != null) {
            personDeathQuery.setDeathDateTimeBefore(diedBeforeDate.toLocalDateTime()); // Timezone?
        }

        OffsetDateTime diedAfterDate = Query.parseDateTime(request.getParameter(AFTER_DATE_PARAMETER));
        if (diedAfterDate != null) {
            personDeathQuery.setDeathDateTimeAfter(diedAfterDate.toLocalDateTime()); // Timezone?
        }
        String pnr = request.getParameter("pnr");
        if (pnr != null) {
            personDeathQuery.setPersonnummer(pnr);
        }
        personDeathQuery.setPageSize(1000000);

        return personDeathQuery;
    }

    @Override
    protected Map<String, String> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        HashMap<String, String> item = new HashMap<>();
        item.put(PNR, person.getPersonnummer());
        //item.put(EFFECTIVE_PNR, person.getPersonnummer());

        OffsetDateTime earliestProdDate = null;

        OffsetDateTime earliestDeathTime = null;

        for (PersonRegistration registration: person.getRegistrations()){
            //for (PersonEffect effect: registration.getEffects()) {
            for (PersonEffect effect: registration.getEffectsAt(filter.effectAt)) {
                for (PersonBaseData data : effect.getDataItems()) {

                    PersonCoreData coreData = data.getCoreData();

                    if (coreData != null) {
                        item.put(EFFECTIVE_PNR, coreData.getCprNumber());
                    }


                    PersonBirthData birthData = data.getBirth();
                    if (birthData != null) {
                        if (birthData.getBirthDatetime() != null) {
                            item.put(BIRTHDAY_YEAR, Integer.toString(birthData.getBirthDatetime().getYear()));
                        }
                        if (birthData.getBirthPlaceCode() != null) {
                            item.put(BIRTH_AUTHORITY, Integer.toString(birthData.getBirthPlaceCode()));
                        }
                        if (birthData.getBirthAuthorityText() != null) {
                            item.put(BIRTH_AUTHORITY_TEXT, Integer.toString(birthData.getBirthAuthorityText()));
                        }
                    }

                    PersonStatusData statusData = data.getStatus();
                    if (statusData != null) {
                        item.put(STATUS_CODE, Integer.toString(statusData.getStatus()));
                        if (statusData.getStatus() == 90) {
                            if (effect.getEffectFrom() != null && (earliestDeathTime == null || effect.getEffectFrom().isBefore(earliestDeathTime))) {
                                earliestDeathTime = effect.getEffectFrom();
                            }
                            if (registration.getRegistrationFrom() != null && (earliestProdDate == null || registration.getRegistrationFrom().isBefore(earliestProdDate))) {
                                earliestProdDate = registration.getRegistrationFrom();
                            }
                        }
                    }

                    PersonCitizenshipData citizenshipData = data.getCitizenship();
                    if (citizenshipData != null) {
                        item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipData.getCountryCode()));
                    }

                    PersonAddressData addressData = data.getAddress();
                    if (addressData != null) {
                        item.put(ROAD_CODE, formatRoadCode(addressData.getRoadCode()));
                        item.put(HOUSE_NUMBER, formatHouseNnr(addressData.getHouseNumber()));
                        item.put(DOOR_NUMBER, addressData.getDoor());
                        item.put(BNR, formatBnr(addressData.getBuildingNumber()));
                        item.put(MUNICIPALITY_CODE, Integer.toString(addressData.getMunicipalityCode()));
                        Lookup lookup = lookupService.doLookup(
                                addressData.getMunicipalityCode(),
                                addressData.getRoadCode(),
                                addressData.getHouseNumber()
                        );
                        if (lookup != null) {
                            item.put(LOCALITY_NAME, lookup.localityName);
                            item.put(LOCALITY_CODE, formatLocalityCode(lookup.localityCode));
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

                    PersonCivilStatusData personCivilStatusData = data.getCivilStatus();
                    if (personCivilStatusData != null) {
                        item.put(SPOUSE_PNR, personCivilStatusData.getSpouseCpr());
                    }

                }
            }
        }
        if (earliestDeathTime != null) {
            item.put(DEATH_DATE, earliestDeathTime.format(dmyFormatter)); // Timezone?
        }
        if (earliestProdDate != null) {
            item.put(PROD_DATE, earliestProdDate.format(dmyFormatter));
        }



        return item;
    }

}