package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
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
import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.SerializationUtils;
import org.hibernate.Session;
import org.springframework.http.HttpStatus;
import org.slf4j.Logger;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.validation.constraints.Null;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.Serializable;
import java.lang.reflect.Field;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.nio.file.StandardOpenOption;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.stream.Stream;

public abstract class StatisticsService {


    protected void get(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {


        // Check that the user has access to CPR data
        DafoUserDetails user = this.getDafoUserManager().getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);
        loggerHelper.info("Incoming request for " + this.getClass().getSimpleName() + " with parameters " + request.getParameterMap());
        this.checkAndLogAccess(loggerHelper);


        this.requireParameter(EFFECT_DATE_PARAMETER, request.getParameter(EFFECT_DATE_PARAMETER));
        Filter filter = new Filter(Query.parseDateTime(request.getParameter(EFFECT_DATE_PARAMETER)));

        final Session primary_session = this.getSessionManager().getSessionFactory().openSession();
        final Session secondary_session = this.getSessionManager().getSessionFactory().openSession();

        try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primary_session);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primary_session, personQuery, PersonEntity.class);

            int written = this.writeItems(this.formatItems(personEntities, secondary_session, filter), response, serviceName);
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
    public static final String FIRST_NAME = "Fornavn";
    public static final String LAST_NAME = "Efternavn";
    public static final String EFFECTIVE_PNR = "PnrGaeld";
    public static final String STATUS_CODE = "Status";
    public static final String CITIZENSHIP_CODE = "StatKod";
    public static final String CIVIL_STATUS = "CivSt";
    public static final String CIVIL_STATUS_DATE = "CivDto";
    public static final String DEATH_DATE = "DoedDto";
    public static final String PROD_DATE = "ProdDto";
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

    public static final String DESTINATION_MUNICIPALITY_CODE = "TilKomKod";
    public static final String DESTINATION_LOCALITY_NAME = "TilLokKortNavn";
    public static final String DESTINATION_ROAD_CODE = "TilVejKod";
    public static final String DESTINATION_HOUSE_NUMBER = "TilHusNr";
    public static final String DESTINATION_FLOOR = "TilEtage";
    public static final String DESTINATION_DOOR_NUMBER = "TilSideDoer";
    public static final String DESTINATION_BNR = "TilBnr";


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

    protected int writeItems(Iterator<Map<String, Object>> items, HttpServletResponse response, ServiceName serviceName) throws IOException {
        CsvSchema.Builder builder = new CsvSchema.Builder();
        builder.setColumnSeparator(';');

        CsvMapper mapper = new CsvMapper();
        mapper.configure(JsonGenerator.Feature.IGNORE_UNKNOWN, true);

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


                                //Routine to write the content to the file
                               /*if(!isFileOn) {
                                String file_name = null;
                                switch (serviceName) {
                                    case BIRTH:
                                        System.out.println("Birth service ran...");
                                        file_name = "birth";
                                        break;
                                    case DEATH:
                                        System.out.println("Death service ran...");
                                        file_name = "death";
                                        break;
                                    case STATUS:
                                        System.out.println("Status service ran...");
                                        file_name = "status";
                                        break;
                                    case MOVEMENT:
                                        System.out.println("Movement service ran...");
                                        file_name = "movement";
                                        break;
                                    default:
                                        System.out.println("No file name assigned!!!");
                                }

                                try {
                                    Iterator<?> iterator = items;
                                    List<String> listValues = new ArrayList<>();
                                    List<Map<String, Object>> itemsList = IteratorUtils.toList(iterator);

                                    //Traversing the items in order to extract columns and values
                                    for (Map<String, Object> element : itemsList) {
                                        for (Map.Entry<String, Object> entry : element.entrySet()) {
                                            //System.out.println("--   Key : " + entry.getKey() + " --   Value : " + entry.getValue());
                                            listValues.add(String.valueOf(entry.getValue()));//Assigns the value
                                        }
                                    }

                                    ObjectWriter writerobj = mapper.writerFor(String.class).with(schema);
                                    File tempFile = new File("c:\\temp\\" + file_name + ".csv");
                                    writerobj.writeValues(tempFile).writeAll(listValues);
                                } catch (IOException e) {
                                    e.printStackTrace();
                                }
                            }*/

        int written;

        for (written = 0; items.hasNext(); written++) {
            writer.write(items.next());


        ObjectWriter writer = this.getCsvMapper().writer(schema);
        SequenceWriter sequenceWriter;

        if (isFileOn) {
            File tempFile = new File(serviceName.name().toLowerCase() + ".csv");
            sequenceWriter = writer.writeValues(tempFile);
        } else {
            sequenceWriter = writer.writeValues(response.getOutputStream());

        }

        int written;
        for (written = 0; items.hasNext(); written++) {
            sequenceWriter.write(items.next());
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
        if (houseNr == null || houseNr.equals("0")) return "";
        return StringUtils.leftPad(houseNr, 4, '0');
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


