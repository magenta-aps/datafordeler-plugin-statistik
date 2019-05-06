package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.person.data.*;
import dk.magenta.datafordeler.statistik.queries.PersonCivilStatusQuery;
import dk.magenta.datafordeler.statistik.utils.CivilStatusFilter;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.Lookup;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.util.*;

import static java.util.stream.Collectors.toSet;


@RestController
@RequestMapping("/statistik/civilstate_data")
public class CivilStatusDataService extends PersonStatisticsService {
    @Autowired
    SessionManager sessionManager;

    @Autowired
    ObjectMapper objectMapper;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    DafoUserManager dafoUserManager;

    @Autowired
    private CprPlugin cprPlugin;

    private String civilStatus;

    private Logger log = LogManager.getLogger(CivilStatusDataService.class.getCanonicalName());


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.CIVILSTATUS);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                CIVIL_STATUS, CIVIL_STATUS_DATE, CITIZENSHIP_CODE, PROD_DATE, PNR, SPOUSE_PNR, "authority", MUNICIPALITY_CODE, BIRTH_AUTHORITY, BIRTH_AUTHORITY_TEXT, BIRTH_AUTHORITY_CODE_TEXT,
                LOCALITY_NAME, LOCALITY_ABBREVIATION, LOCALITY_CODE, ROAD_CODE, HOUSE_NUMBER, FLOOR_NUMBER, DOOR_NUMBER, BNR

        });
    }

    @Override
    protected Filter getFilter(HttpServletRequest request) {
        return new CivilStatusFilter(request);
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
        return new String[]{};
    }

    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @Override
    protected PersonCivilStatusQuery getQuery(Filter filter) {
        return new PersonCivilStatusQuery((CivilStatusFilter) filter);
    }


    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        List<Map<String, String>> itemMap = this.formatPersonByRecord(person, session, lookupService, (CivilStatusFilter) filter);
        if (itemMap == null || itemMap.isEmpty()) {
            return Collections.emptyList();
        }
        return itemMap;
    }

    protected List<Map<String, String>> formatPersonByRecord(PersonEntity person, Session session, LookupService lookupService, CivilStatusFilter filter) {

        List<Map<String, String>> itemMap = new ArrayList<Map<String, String>>();

        OffsetDateTime searchTime = filter.registrationAfter;
        OffsetDateTime mariageEffectTime = null;

        String birthAuthorityId;
        String birthAuthorityText;
        String birthAuthorityCode;

        BirthPlaceDataRecord birthPlaceDataRecord = person.getBirthPlace().iterator().next();
        birthAuthorityId = Integer.toString(birthPlaceDataRecord.getAuthority());
        birthAuthorityText = birthPlaceDataRecord.getBirthPlaceName();
        birthAuthorityCode = Integer.toString(birthPlaceDataRecord.getBirthPlaceCode());

        Set<CivilStatusDataRecord> civilStatusses = null;
        if (filter.getCivilStatus() != null || searchTime != null) {
            civilStatusses = person.getCivilstatus().stream().filter(r -> (filter.getCivilStatus()== null || filter.getCivilStatus().equals(r.getCivilStatus())) &&
                    r.getBitemporality().effectFrom!=null && r.getBitemporality().effectFrom.isAfter(searchTime)
            ).collect(toSet());
        } else {
            civilStatusses = person.getCivilstatus();
        }

        for (CivilStatusDataRecord civilStatusDataRecord : civilStatusses) {
            mariageEffectTime = civilStatusDataRecord.getEffectFrom();

            HashMap<String, String> item = new HashMap<>();
            item.put(PNR, person.getPersonnummer());

            item.put(CIVIL_STATUS, civilStatusDataRecord.getCivilStatus());


            item.put(SPOUSE_PNR, civilStatusDataRecord.getSpouseCpr());
            item.put("authority", Integer.toString(civilStatusDataRecord.getAuthority()));

            item.put(BIRTH_AUTHORITY, birthAuthorityId);
            item.put(BIRTH_AUTHORITY_TEXT, birthAuthorityText);
            item.put(BIRTH_AUTHORITY_CODE_TEXT, birthAuthorityCode);

            if (mariageEffectTime != null) {
                item.put(CIVIL_STATUS_DATE, formatTime(mariageEffectTime));
            }

            if (civilStatusDataRecord.getRegistrationFrom() != null) {
                item.put(PROD_DATE, formatTime(civilStatusDataRecord.getRegistrationFrom().atZoneSameInstant(cprDataOffset)));
            }
            if (civilStatusDataRecord.getOriginDate() != null) {
                item.put(FILE_DATE, formatTime(civilStatusDataRecord.getOriginDate()));
            }

            AddressDataRecord addressDataRecord = findNewestAfterFilterOnEffect(person.getAddress(), mariageEffectTime);
            if (addressDataRecord != null) {
                int municipalityCode = addressDataRecord.getMunicipalityCode();
                item.put(MUNICIPALITY_CODE, Integer.toString(municipalityCode));
                item.put(ROAD_CODE, formatRoadCode(addressDataRecord.getRoadCode()));
                item.put(HOUSE_NUMBER, formatHouseNnr(addressDataRecord.getHouseNumber()));
                item.put(FLOOR_NUMBER, addressDataRecord.getFloor());
                item.put(DOOR_NUMBER, addressDataRecord.getDoor());
                item.put(BNR, formatBnr(addressDataRecord.getBuildingNumber()));
                Lookup lookup = lookupService.doLookup(
                        addressDataRecord.getMunicipalityCode(),
                        addressDataRecord.getRoadCode(),
                        addressDataRecord.getHouseNumber()
                );
                if (lookup != null) {
                    if (lookup.localityName != null) {
                        item.put(LOCALITY_NAME, lookup.localityName);
                    }
                    if (lookup.localityAbbrev != null) {
                        item.put(LOCALITY_ABBREVIATION, lookup.localityAbbrev);
                    }
                    if (lookup.localityCode != 0) {
                        item.put(LOCALITY_CODE, formatLocalityCode(lookup.localityCode));
                    }
                }
            }

            CitizenshipDataRecord citizenshipDataRecord = findNewestAfterFilterOnEffect(person.getCitizenship(), mariageEffectTime);
            if (citizenshipDataRecord != null) {
                item.put(CITIZENSHIP_CODE, Integer.toString(citizenshipDataRecord.getCountryCode()));
            }

            replaceMapValues(item, null, "");
            itemMap.add(item);

        }
        return itemMap;
    }
}