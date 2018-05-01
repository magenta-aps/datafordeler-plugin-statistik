package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
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
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.apache.commons.lang.StringUtils;
import org.hibernate.Session;
import org.slf4j.Logger;
import org.springframework.http.HttpStatus;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.IOException;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public abstract class StatisticsService {


    protected void get(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {


        // Check that the user has access to CPR data
        DafoUserDetails user = this.getDafoUserManager().getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);
        loggerHelper.info("Incoming request for " + this.getClass().getSimpleName() + " with parameters " + request.getParameterMap());
        this.checkAndLogAccess(loggerHelper);

        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = this.getFilter(request);

        final Session primarySession = this.getSessionManager().getSessionFactory().openSession();
        final Session secondarySession = this.getSessionManager().getSessionFactory().openSession();

        primarySession.setDefaultReadOnly(true);
        secondarySession.setDefaultReadOnly(true);

        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primarySession);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primarySession, personQuery, PersonEntity.class);

            int written = this.writeItems(this.formatItems(personEntities, secondarySession, filter), response, serviceName);
            if (written == 0) {
                response.sendError(HttpStatus.NO_CONTENT.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            primarySession.close();
            secondarySession.close();
        }
    }

    protected abstract List<String> getColumnNames();

    protected abstract SessionManager getSessionManager();

    protected abstract CsvMapper getCsvMapper();

    protected abstract DafoUserManager getDafoUserManager();

    protected abstract Logger getLogger();

    protected abstract Map<String, String> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter);

    protected Filter getFilter(HttpServletRequest request) {
        return new Filter(Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER)));
    }

    public enum ServiceName {
        BIRTH,
        DEATH,
        MOVEMENT,
        STATUS;
    }

    public static boolean isFileOn = true;

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
    public static final String BEFORE_DATE_PARAMETER = "beforeDate";
    public static final String AFTER_DATE_PARAMETER = "afterDate";
    public static final String EFFECT_DATE_PARAMETER = "effectDate";

    //Column names for person
    public static final String PNR = "Pnr";
    public static final String BIRTHDAY_YEAR = "FoedAar";
    public static final String BIRTH_AUTHORITY = "FoedMynKod";
    public static final String BIRTH_AUTHORITY_TEXT = "FoedMynKodTxt";
    public static final String FIRST_NAME = "Fornavn";
    public static final String LAST_NAME = "Efternavn";
    public static final String EFFECTIVE_PNR = "PnrGaeld";
    public static final String STATUS_CODE = "Status";
    public static final String CITIZENSHIP_CODE = "StatKod";
    public static final String CIVIL_STATUS = "CivSt";
    public static final String CIVIL_STATUS_DATE = "CivDto";
    public static final String CIVIL_STATUS_PROD_DATE = "CivProdDto";
    public static final String DEATH_DATE = "DoedDto";
    public static final String PROD_DATE = "ProdDto";
    public static final String MOVE_PROD_DATE = "FlytProdDto";
    public static final String MUNICIPALITY_CODE = "KomKod";
    public static final String LOCALITY_NAME = "LokNavn";
    public static final String LOCALITY_CODE = "LokKode";
    public static final String LOCALITY_ABBREVIATION = "LokKortNavn";
    public static final String ROAD_CODE = "VejKod";
    public static final String HOUSE_NUMBER = "HusNr";
    public static final String DOOR_NUMBER = "SideDoer";
    public static final String FLOOR_NUMBER = "Etage";
    public static final String BNR = "Bnr";
    public static final String MOVING_IN_DATE = "TilFlyDto";
    public static final String MOVE_DATE = "FlyDto";
    public static final String POST_CODE = "Postnr";
    public static final String CHURCH = "Kirke";


    public static final String ORIGIN_MUNICIPALITY_CODE = "FraKomKod";
    public static final String ORIGIN_LOCALITY_NAME = "FraLokKortNavn";
    public static final String ORIGIN_ROAD_CODE = "FraVejKod";
    public static final String ORIGIN_HOUSE_NUMBER = "FraHusNr";
    public static final String ORIGIN_FLOOR = "FraEtage";
    public static final String ORIGIN_DOOR_NUMBER = "FraSideDoer";
    public static final String ORIGIN_BNR = "FraBnr";
    public static final String ORIGIN_COUNTRY_CODE = "FraLand";

    public static final String DESTINATION_MUNICIPALITY_CODE = "TilKomKod";
    public static final String DESTINATION_LOCALITY_NAME = "TilLokKortNavn";
    public static final String DESTINATION_ROAD_CODE = "TilVejKod";
    public static final String DESTINATION_HOUSE_NUMBER = "TilHusNr";
    public static final String DESTINATION_FLOOR = "TilEtage";
    public static final String DESTINATION_DOOR_NUMBER = "TilSideDoer";
    public static final String DESTINATION_BNR = "TilBnr";
    public static final String DESTINATION_COUNTRY_CODE = "TilLand";


    //Column names for parent mother person
    public static final String MOTHER_PREFIX = "M_";
    public static final String MOTHER_PNR = MOTHER_PREFIX + PNR;

    //Column names for parent father person
    public static final String FATHER_PREFIX = "F_";
    public static final String FATHER_PNR = FATHER_PREFIX + PNR;

    //Column names for  spouse person
    public static final String SPOUSE_PNR = "AegtePnr";


    protected PersonQuery getQuery(HttpServletRequest request) {
        OffsetDateTime livingInGreenlandAtDate = Query.parseDateTime(request.getParameter(INCLUSION_DATE_PARAMETER));
        PersonQuery personQuery = new PersonQuery();
        if (livingInGreenlandAtDate != null) {
            personQuery.setEffectFrom(livingInGreenlandAtDate);
            personQuery.setEffectTo(livingInGreenlandAtDate);
        }
        return personQuery;
    }

    protected int writeItems(Iterator<Map<String, String>> items, HttpServletResponse response, ServiceName serviceName) throws IOException {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        builder.setColumnSeparator(';');

        CsvMapper mapper = this.getCsvMapper();
        mapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);
        mapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_STRINGS, true);
        mapper.configure(CsvGenerator.Feature.ALWAYS_QUOTE_EMPTY_STRINGS, true);

        List<String> keys = this.getColumnNames();
        for (int i = 0; i < keys.size(); i++) {
            builder.addColumn(new CsvSchema.Column(
                    i, keys.get(i),
                    CsvSchema.ColumnType.STRING
            ));
        }
        CsvSchema schema = builder.build().withHeader();


        response.setContentType("text/csv");

        SequenceWriter writer;
        ObjectWriter writerobj = mapper.writer(schema);
        String outputDescription = null;

        /*TODO: Proposal for the sake of defining a better directory structure.
        * For example:
        * ../statistik/birth/birth_timestamp.csv
        * ../statistik/death/death_timestamp.csv
        * ../statistik/status/status_timestamp.csv
        * ../statistik/movement/movement_timestamp.csv  */

        if (isFileOn) {
            //Get current date time
            LocalDateTime now = LocalDateTime.now();
            DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
            String formatDateTime = now.format(formatter);

            //Directory and file creation
            File folder = new File(System.getProperty("user.home") + File.separator + "statistik");
            if (!folder.exists()) {
                folder.mkdirs();
            }

            File file = new File(folder, serviceName.name().toLowerCase() +"_" + formatDateTime.toString() +".csv");
            file.createNewFile();
            writer = writerobj.writeValues(file);
            outputDescription = "Written to file " + file.getCanonicalPath();
        } else {
            writer = writerobj.writeValues(response.getOutputStream());
            outputDescription = "Written to response";
        }

        int written;
        for (written = 0; items.hasNext(); written++) {
            Object item = items.next();
            if (item != null) {
                writer.write(item);
            }
        }
        writer.close();
        System.out.println(outputDescription);

        return written;
    }

    public Iterator<Map<String, String>> formatItems(Stream<PersonEntity> personEntities, Session session, Filter filter) {
        LookupService lookupService = new LookupService(session);
        return personEntities.map(personEntity -> formatPerson(personEntity, session, lookupService, filter)).iterator();
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
        if (houseNr == null || houseNr.equals("0")) return "";
        return StringUtils.leftPad(houseNr, 4, '0');
    }

    protected static String formatLocalityCode(int localityCode) {
        if (localityCode == 0) return "";
        return String.format("%04d", localityCode);
    }

    protected static String string(int value) {
        return Integer.toString(value);
    }

    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
        } catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw (e);
        }
    }
}


