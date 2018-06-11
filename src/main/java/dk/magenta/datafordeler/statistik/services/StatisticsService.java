package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEffect;
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
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.stream.Stream;

public abstract class StatisticsService {

    public static final ZoneId cprDataOffset = ZoneId.of("Europe/Copenhagen");

    private class Counter {
        public long count = 0;
    }

    public static String PATH_FILE = null;

    static {
        StatisticsService.PATH_FILE = System.getProperty("user.home") + File.separator + "statistik";
        File folder = new File(StatisticsService.PATH_FILE);
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    protected String[] requiredParameters() {
        return new String[]{};
    }


    protected void get(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException {


        // Check that the user has access to CPR data
          DafoUserDetails user = this.getDafoUserManager().getUserFromRequest(request);
        LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);
        loggerHelper.info("Incoming request for " + this.getClass().getSimpleName() + " with parameters " + request.getParameterMap());
        this.checkAndLogAccess(loggerHelper);
        for (String required : this.requiredParameters()) {
            this.requireParameter(required, request.getParameter(required));
        }
        Filter filter = this.getFilter(request);

        final Session primarySession = this.getSessionManager().getSessionFactory().openSession();
        final Session secondarySession = this.getSessionManager().getSessionFactory().openSession();

        primarySession.setDefaultReadOnly(true);
        secondarySession.setDefaultReadOnly(true);

        //New code-------------
        List<PersonQuery> queries;
        try {
           queries = this.getQueryList(request);
            Stream<PersonEntity> personEntities = null;
            Stream<PersonEntity> concatenation = null;
            for (PersonQuery query : queries) {
                //here the stream should be placed
                  personEntities = QueryManager.getAllEntitiesAsStream(primarySession, query, PersonEntity.class);
                System.out.println("Count entities : "+personEntities.count());
                  //There most be a concatenation mechanism


            }

            personEntities.forEach(System.out::println);


        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            primarySession.close();
            secondarySession.close();
        }

        //New code end-------------


       /* try {
            PersonQuery personQuery = this.getQuery(request);
            personQuery.applyFilters(primarySession);
            Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primarySession, personQuery, PersonEntity.class);

            final Counter counter = new Counter();
            int written = this.writeItems(this.formatItems(personEntities, secondarySession, filter), response, serviceName, item -> {
                counter.count++;
                if (counter.count > 100) {
                    primarySession.clear();
                    secondarySession.clear();
                    counter.count = 0;
                }
            });
            if (written == 0) {
                response.sendError(HttpStatus.NO_CONTENT.value());
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            primarySession.close();
            secondarySession.close();
        }*/
    }

    protected abstract List<String> getColumnNames();

    protected abstract SessionManager getSessionManager();

    protected abstract CsvMapper getCsvMapper();

    protected abstract DafoUserManager getDafoUserManager();

    protected abstract Logger getLogger();

    protected abstract List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter);

    protected Filter getFilter(HttpServletRequest request) {
        return new Filter(request);
    }


    public enum ServiceName {
        BIRTH,
        DEATH,
        MOVEMENT,
        STATUS,
        ADDRESS;
    }

    public static boolean isFileOn = true;
    public static boolean isFileUploaded = false;

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
    public static final String BEFORE_DATE_PARAMETER = "beforeDate";
    public static final String AFTER_DATE_PARAMETER = "afterDate";
    public static final String EFFECT_DATE_PARAMETER = "effectDate";

    public static final String REGISTRATION_AFTER = "registrationAfter";
    public static final String REGISTRATION_BEFORE = "registrationBefore";

    //Column names for person
    public static final String PNR = "Pnr";
    public static final String BIRTHDAY_YEAR = "FoedAar";
    public static final String BIRTH_AUTHORITY = "FoedMynKod";
    public static final String BIRTH_AUTHORITY_CODE_TEXT = "FoedMynKodTxt";
    public static final String BIRTH_AUTHORITY_TEXT = "FoedMynTxt";
    public static final String FIRST_NAME = "Fornavn";
    public static final String MIDDLE_NAME = "Mellemnavn";
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
    public static final String ROAD_NAME = "VejNavn";
    public static final String HOUSE_NUMBER = "HusNr";
    public static final String DOOR_NUMBER = "SideDoer";
    public static final String FLOOR_NUMBER = "Etage";
    public static final String BNR = "Bnr";
    public static final String MOVING_IN_DATE = "TilFlyDto";
    public static final String MOVE_DATE = "FlyDto";
    public static final String POST_CODE = "Postnr";
    public static final String POST_DISTRICT = "PostDistrikt";
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

    protected List<PersonQuery> getQueryList(HttpServletRequest request) throws IOException {
        //The name or path of the file must be here
        File inFile = new File("C:\\Users\\EFRIN.GONZALEZ\\Downloads\\inFile.csv");
        //String inFile = "/home/lars/tmp/foo.txt";
        ArrayList<String> pnrs = new ArrayList<>();
        try (Stream<String> stream = Files.lines(Paths.get(inFile.toString()))) {
            stream.forEach(pnrs::add);
        }
        System.out.println(pnrs.size() + " pnrs loaded");

        int count = 0;
        ArrayList<PersonQuery> queries = new ArrayList<>();
        PersonQuery personQuery = new PersonQuery();
        for (String pnr : pnrs) {
            count++;
            personQuery.addPersonnummer(pnr);
            if (count >= 1000) {
                queries.add(personQuery);
                personQuery = new PersonQuery();
                count = 0;
            }
        }
        if (count > 0) {
            queries.add(personQuery);
        }

        return queries;
    }



    protected int writeItems(Iterator<Map<String, String>> items, HttpServletResponse response, ServiceName serviceName, Consumer<Object> afterEach) throws IOException {
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
        int written = 0;

        if (items.hasNext()) {
            response.setContentType("text/csv");


            SequenceWriter writer = null;
            ObjectWriter writerobj = mapper.writer(schema);
            String outputDescription = null;
            

            if (isFileOn) {
                //Get current date time
                LocalDateTime now = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
                String formatDateTime = now.format(formatter);
                if (PATH_FILE != null) {
                    System.out.println(PATH_FILE);
                    File file = new File(PATH_FILE, serviceName.name().toLowerCase() + "_" + formatDateTime.toString() + ".csv");
                    file.createNewFile();

                            //-------------New code--------------------------
                            //-----------------------------------------------
  /*                              if(isFileUploaded){

                                    Session session = this.getSessionManager().getSessionFactory().openSession();
                                    Session lookupSession = this.getSessionManager().getSessionFactory().openSession();
                                    LookupService lookupService = new LookupService(lookupSession);

                                    session.setDefaultReadOnly(true);
                                    OffsetDateTime time = OffsetDateTime.now();
                                    ArrayList<Map<String, String>> items_ = new ArrayList<>();
                                    Filter filter = new Filter();
                                    filter.effectAt = OffsetDateTime.now();
                                    try {
                                        for (PersonQuery query : queries) {
                                            query.setPageSize(1000);
                                            List<PersonEntity> personEntities = QueryManager.getAllEntities(session, query, PersonEntity.class);

                                            for (PersonEntity personEntity : personEntities) {
                                                items_.add((Map<String, String>) this.formatPerson(personEntity, lookupSession, lookupService, filter));
                                            }
                                        }
                                    } finally {
                                        session.close();
                                    }

                                }
*/
                                //-----------------------------------------------
                                //-------------End New code----------------------


                    writer = writerobj.writeValues(file);
                    outputDescription = "Written to file " + file.getCanonicalPath();
                }

            } else {
                writer = writerobj.writeValues(response.getOutputStream());
                outputDescription = "Written to response";
            }

            for (written = 0; items.hasNext(); written++) {
                Object item = items.next();
                if (item != null) {
                    writer.write(item);
                }
                afterEach.accept(item);
            }
            writer.close();
            System.out.println(outputDescription);
        }

        return written;
    }

    public Iterator<Map<String, String>> formatItems(Stream<PersonEntity> personEntities, Session lookupSession, Filter filter) {
        LookupService lookupService = new LookupService(lookupSession);
        return personEntities.flatMap(
                personEntity -> formatPerson(personEntity, lookupSession, lookupService, filter).stream()
        ).iterator();
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

    public class EffectComparator<T extends Effect> implements Comparator<T> {
        @Override
        public int compare(T o1, T o2) {
            int c = o1.compareTo(o2);
            if (c == 0) {
                c = o1.getRegistration().compareTo(o2.getRegistration());
            }
            return c;
        }
    }

    public final EffectComparator<PersonEffect> personComparator = new EffectComparator<PersonEffect>();

}


