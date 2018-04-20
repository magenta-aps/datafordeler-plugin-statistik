package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.commons.lang.StringUtils;
import org.hibernate.Session;
import org.springframework.http.HttpStatus;
import org.slf4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.IOException;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class StatisticsService {

    protected void get(HttpServletRequest request, HttpServletResponse response) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {

        // Check that the user has access to CPR data
        DafoUserDetails user = this.getDafoUserManager().getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);
        loggerHelper.info("Incoming request for "+this.getClass().getSimpleName()+" with parameters " + request.getParameterMap());
        this.checkAndLogAccess(loggerHelper);


        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER)));

        final Session primary_session = this.getSessionManager().getSessionFactory().openSession();
        final Session secondary_session = this.getSessionManager().getSessionFactory().openSession();

        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            int written = 0;//this.writeItems(this.formatItems(personEntities, secondary_session, filter), response);
            if (written == 0) {
                response.sendError(HttpStatus.NO_CONTENT.value());
            }
        } finally {
            primary_session.close();
            secondary_session.close();
        }
    }

    protected abstract List<String> getColumnNames();

    protected abstract SessionManager getSessionManager();

    protected abstract CsvMapper getCsvMapper();

    protected abstract DafoUserManager getDafoUserManager();

    protected abstract Logger getLogger();

    protected abstract Map<String, Object> formatPerson(PersonEntity person, Session session, Filter filter);

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
    public static final String BEFORE_DATE_PARAMETER = "beforeDate";
    public static final String AFTER_DATE_PARAMETER = "afterDate";
    public static final String EFFECT_DATE_PARAMETER = "effectDate";

    //Column names for person
    public static final String PNR = "pnr";
    public static final String BIRTHDAY_YEAR ="birth_year";
    public static final String BIRTH_AUTHORITY = "birth_authority";
    public static final String FIRST_NAME = "first_name";
    public static final String LAST_NAME = "last_name";
    public static final String EFFECTIVE_PNR = "effective_pnr";
    public static final String STATUS_CODE = "status_code";
    public static final String CIVIL_STATUS = "civil_status";
    public static final String CIVIL_STATUS_DATE = "civil_status_date";
    public static final String DEATH_DATE = "death_date";
    public static final String PROD_DATE = "prod_date";
    public static final String MUNICIPALITY_CODE = "municipality_code";
    public static final String LOCALITY_NAME = "locality_name";
    public static final String LOCALITY_CODE = "locality_code";
    public static final String LOCALITY_ABBREVIATION = "locality_abbrev";
    public static final String ROAD_CODE = "road_code";
    public static final String HOUSE_NUMBER = "house_number";
    public static final String DOOR_NUMBER = "door_number";
    public static final String FLOOR_NUMBER = "floor_number";
    public static final String BNR = "bnr";
    public static final String MOVING_IN_DATE = "moving_in_date";
    public static final String MOVE_DATE = "move_date";
    public static final String POST_CODE =  "post_code";
    public static final String CHURCH = "church";

    public static final String ORIGIN_MUNICIPALITY_CODE = "origin_municipality_code";
    public static final String ORIGIN_LOCALITY_NAME = "origin_locality_name";
    public static final String ORIGIN_ROAD_CODE = "origin_road_code";
    public static final String ORIGIN_HOUSE_NUMBER = "origin_house_number";
    public static final String ORIGIN_FLOOR = "origin_floor";
    public static final String ORIGIN_DOOR_NUMBER = "origin_door_number";
    public static final String ORIGIN_BNR = "origin_bnr";

    public static final String DESTINATION_MUNICIPALITY_CODE = "destination_municipality_code";
    public static final String DESTINATION_LOCALITY_NAME = "destination_locality_name";
    public static final String DESTINATION_ROAD_CODE = "destination_road_code";
    public static final String DESTINATION_HOUSE_NUMBER = "destination_house_number";
    public static final String DESTINATION_FLOOR = "destination_floor";
    public static final String DESTINATION_DOOR_NUMBER = "destination_door_number";
    public static final String DESTINATION_BNR = "destination_bnr";

    //Column names for parent mother person
    public static final String MOTHER_PREFIX = "mother_";
    public static final String MOTHER_PNR = "mother_pnr";
    public static final String MOTHER_BIRTH_AUTHORIRTY = "mother_birth_authority";
    public static final String MOTHER_STATUS_CODE = "mother_status_code";
    public static final String MOTHER_MUNICIPALITY_CODE = "mother_municipality_code";
    public static final String MOTHER_LOCALITY_CODE = "mother_locality_code";
    public static final String MOTHER_LOCALITY_NAME = "mother_locality_name";
    public static final String MOTHER_ROAD_CODE = "mother_road_code";
    public static final String MOTHER_HOUSE_NUMBER = "mother_house_number";
    public static final String MOTHER_DOOR_NUMBER = "mother_door_number";
    public static final String MOTHER_BNR = "mother_bnr";

    //Column names for parent father person
    public static final String FATHER_PREFIX = "father_";
    public static final String FATHER_PNR = "father_pnr";
    public static final String FATHER_BIRTH_AUTHORIRTY = "father_birth_authority";
    public static final String FATHER_STATUS_CODE = "father_status_code";
    public static final String FATHER_MUNICIPALITY_CODE = "father_municipality_code";
    public static final String FATHER_LOCALITY_CODE = "father_locality_code";
    public static final String FATHER_LOCALITY_NAME = "father_locality_name";
    public static final String FATHER_ROAD_CODE = "father_road_code";
    public static final String FATHER_HOUSE_NUMBER = "father_house_number";
    public static final String FATHER_DOOR_NUMBER = "father_door_number";
    public static final String FATHER_BNR = "father_bnr";

    //Column names for  spouse person
    public static final String SPOUSE_PNR = "spouse_pnr";






    protected PersonQuery getQuery(HttpServletRequest request) {
        OffsetDateTime livingInGreenlandAtDate = Query.parseDateTime(request.getParameter(INCLUSION_DATE_PARAMETER));
        PersonQuery personQuery = new PersonQuery();
        if (livingInGreenlandAtDate != null) {
            personQuery.setEffectFrom(livingInGreenlandAtDate);
            personQuery.setEffectTo(livingInGreenlandAtDate);
        }
        return personQuery;
    }

    protected int writeItems(Iterator<Map<String, Object>> items, HttpServletResponse response) throws IOException {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        builder.setColumnSeparator(';');
        List<String> keys = this.getColumnNames();
        for (int i = 0; i < keys.size(); i++) {
            builder.addColumn(new CsvSchema.Column(
                    i, keys.get(i),
                    CsvSchema.ColumnType.STRING
            ));
        }
        CsvSchema schema = builder.build().withHeader();
        response.setContentType("text/csv");
        SequenceWriter writer = this.getCsvMapper().writer(schema).writeValues(response.getOutputStream());
        int written;
        for (written = 0; items.hasNext(); written++) {
            writer.write(items.next());
        }
        return written;
    }

    public Iterator<Map<String, Object>> formatItems(Stream<PersonEntity> personEntities, Session secondary_session, Filter filter) {
        return personEntities.map(personEntity -> formatPerson(personEntity, secondary_session, filter)).iterator();
    }

    protected void requireParameter(String parameterName, String parameterValue) throws MissingParameterException {
        if (parameterValue == null) {
            throw new MissingParameterException(parameterName);
        }
    }

    protected static DateTimeFormatter dmyFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    protected static String formatRoadCode(Integer roadCode) {
        return roadCode != null ? String.format("%04d", roadCode) : null;
    }

    protected static String formatStatusCode(int statusCode) {
        return String.format("%02d", statusCode);
    }

    protected static String formatBnr(String bnr) {
        return (bnr != null && !bnr.isEmpty()) ? StringUtils.leftPad(bnr, 4, '0') : "";
    }

    protected static String formatHouseNnr(String houseNr) {
        return StringUtils.leftPad(houseNr, 4, '0');
    }

    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
        }
        catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw(e);
        }
    }
}


