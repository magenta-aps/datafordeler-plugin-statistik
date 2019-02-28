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

    private Logger log = LogManager.getLogger(DeathDataService.class.getCanonicalName());


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.CIVILSTATUS);
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                CIVIL_STATUS, "civilDate", PROD_DATE, PNR, SPOUSE_PNR, "authority", MUNICIPALITY_CODE, BIRTH_AUTHORITY, BIRTH_AUTHORITY_TEXT,
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
        return new String[]{"registrationAfter", CIVIL_STATUS};
    }

    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @Override
    protected PersonCivilStatusQuery getQuery(Filter filter) {
        return new PersonCivilStatusQuery((CivilStatusFilter)filter);
    }


    @Override
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        List<Map<String, String>> itemMap = this.formatPersonByRecord(person, session, lookupService, filter);
        if (itemMap == null || itemMap.isEmpty()) {
            return Collections.emptyList();
        }
        //List<HashMap<String, String>> item = new List<HashMap>(itemMap);
        //item.put(PNR, person.getPersonnummer());
        return itemMap;
    }

    protected List<Map<String, String>> formatPersonByRecord(PersonEntity person, Session session, LookupService lookupService, Filter filter) {

        List<Map<String, String>> itemMap = new ArrayList<Map<String, String>>();


        OffsetDateTime searchTime = filter.registrationAfter;
        OffsetDateTime mariageEffectTime = null;

        String birthAuthorityId;
        String birthAuthorityText;
        String birthAuthorityCode;

        /*for (BirthPlaceDataRecord birthPlaceDataRecord : sortRecords(person.getBirthPlace())) {
            if (birthPlaceDataRecord.getBitemporality().registrationTo == null && birthPlaceDataRecord.getBitemporality().containsEffect(deathEffectTime, deathEffectTime)) {
                birthAuthorityId = Integer.toString(birthPlaceDataRecord.getAuthority());
                birthAuthorityText = birthPlaceDataRecord.getBirthPlaceName();
                birthAuthorityCode = Integer.toString(birthPlaceDataRecord.getBirthPlaceName());
            }
        }*/

        for (CivilStatusDataRecord civilStatusDataRecord : sortRecords(person.getCivilstatus())) {
            if (filter.registrationAfter.isBefore(civilStatusDataRecord.getEffectFrom())) {


                if (civilStatusDataRecord.getCivilStatus().equals("G")) {

                    HashMap<String, String> item = new HashMap<>();
                    item.put(PNR, person.getPersonnummer());

                    item.put(CIVIL_STATUS, civilStatusDataRecord.getCivilStatus());
                    mariageEffectTime = civilStatusDataRecord.getEffectFrom();
                    item.put("civilDate", formatTime(mariageEffectTime));
                    item.put(SPOUSE_PNR, civilStatusDataRecord.getSpouseCpr());
                    item.put("authority", Integer.toString(civilStatusDataRecord.getAuthority()));




                    for (AddressDataRecord addressDataRecord : sortRecords(person.getAddress())) {
                        if (addressDataRecord.getBitemporality().containsEffect(mariageEffectTime, mariageEffectTime) ) {
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
                    }


                    replaceMapValues(item, null, "");

                    itemMap.add(item);






                }
            }
        }




        return itemMap;
    }

}