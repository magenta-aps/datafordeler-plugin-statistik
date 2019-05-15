package dk.magenta.datafordeler.statistik.services;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.http.HttpStatus;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class StatisticsService {

    public static final ZoneId cprDataOffset = ZoneId.of("Europe/Copenhagen");

    public static String PATH_FILE = null;

    static {
        //StatisticsService.PATH_FILE = System.getProperty("user.home") + File.separator + "statistik";
        StatisticsService.PATH_FILE = "statistik";
        File folder = new File(StatisticsService.PATH_FILE);
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    protected String[] requiredParameters() {
        return new String[]{};
    }

    protected DafoUserDetails getUser(HttpServletRequest request) throws InvalidTokenException, AccessDeniedException, InvalidCertificateException {
        return this.getDafoUserManager().getUserFromRequest(request);
    }


    public abstract int run(Filter filter, OutputStream outputStream);
    
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {
        // Check that the user has access to CPR data
        DafoUserDetails user = this.getUser(request);
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

        try {
            response.setContentType("text/csv");
            String outputDescription = null;
            OutputStream outputStream = null;

            if (this.getWriteToLocalFile()) {
                //Get current date time

                LocalDateTime now = LocalDateTime.now();
                DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
                String formatDateTime = now.format(formatter);

                if (PATH_FILE != null) {
                    File file = new File(PATH_FILE, serviceName.name().toLowerCase() + "_" + formatDateTime + ".csv");
                    file.createNewFile();
                    outputStream = new FileOutputStream(file);
                    outputDescription = "Written to file " + file.getCanonicalPath();
                }

            } else {
                response.setHeader("Content-Disposition", "attachment; filename=\"response.csv\"");
                outputStream = response.getOutputStream();
                outputDescription = "Written to response";
            }

            if (outputStream != null) {
                int written = this.run(filter, outputStream);
                this.getLogger().info(outputDescription);
                if (written == 0) {
                    response.sendError(HttpStatus.NO_CONTENT.value());
                }
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

    protected Filter getFilter(HttpServletRequest request) {
        return new Filter(request);
    }


    public enum ServiceName {
        BIRTH,
        DEATH,
        CIVILSTATUS,
        MOVEMENT,
        STATUS,
        ADDRESS,
        ROAD,
        LOCALITY;
    }

    private boolean writeToLocalFile = true;

    public void setWriteToLocalFile(boolean writeToLocalFile) {
        this.writeToLocalFile = writeToLocalFile;
    }

    public boolean getWriteToLocalFile() {
        return this.writeToLocalFile;
    }
    //TODO: is the person living in Greenland?
    //TODO: how can control the deletion of the file? could it be with an expiration date flag?
    //TODO: Can limit the IP address in order to access the endpoints?

    public static final String INCLUSION_DATE_PARAMETER = "inclusionDate";
    public static final String BEFORE_DATE_PARAMETER = "beforeDate";
    public static final String AFTER_DATE_PARAMETER = "afterDate";
    public static final String EFFECT_DATE_PARAMETER = "effectDate";
    public static final String ONLY_PNR = "pnr";

    public static final String REGISTRATION_AFTER = "registrationAfter";
    public static final String REGISTRATION_BEFORE = "registrationBefore";
    public static final String REGISTRATION_AT = "registrationAt";

    public static final String ORIGIN_AFTER = "originAfter";
    public static final String ORIGIN_BEFORE = "originBefore";

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
    public static final String FILE_DATE = "ProdFilDto";
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
    public static final String BYGDE = "Bygde";

    public static final String MUNICIPALITY_SHORT_NAME = "KomKortNavn";
    public static final String MUNICIPALITY_NAME = "KomNavn";
    public static final String LOC_TYPE_CODE = "LokTypeKod";
    public static final String LOC_TYPE_NAME = "LokTypeNavn";
    public static final String LOC_STATUS_CODE = "LokStatusKod";
    public static final String LOC_STATUS_NAME = "LokStatusNavn";

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

    //Column names for  guardian person
    public static final String NO_OF_GUARDIANS = "Guardians";
    public static final String GUARDIAN_PNR = "GuardianPnr";

    public static final String PROTECTION_TYPE = "ProtectionType";

    protected int writeItems(Iterator<Map<String, String>> items, OutputStream outputStream, Consumer<Object> afterEach) throws IOException {
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
            ObjectWriter writerobj = mapper.writer(schema);

            SequenceWriter writer = writerobj.writeValues(outputStream);
            for (written = 0; items.hasNext(); written++) {
                Object item = items.next();
                if (item != null) {
                    writer.write(item);
                }
                afterEach.accept(item);
            }
            writer.close();
        }
        return written;
    }

    protected void requireParameter(String parameterName, String parameterValue) throws MissingParameterException {
        if (parameterValue == null) {
            throw new MissingParameterException(parameterName);
        }
    }

    protected static DateTimeFormatter dmyFormatter = DateTimeFormatter.ofPattern("dd-MM-yyyy");

    protected String formatPnr(String pnr) {
        if (pnr == null || pnr.isEmpty() || pnr.equals("0000000000")) {
            return "";
        }
        return pnr;
    }

    protected static String formatMunicipalityCode(Integer municipalityCode) {
        return municipalityCode != null ? String.format("%04d", municipalityCode) : null;
    }

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
        if (houseNr == null || houseNr.isEmpty()) return "";
        return StringUtils.leftPad(houseNr, 4, '0');
    }

    protected static String formatLocalityCode(int localityCode) {
        if (localityCode == 0) return "";
        return String.format("%04d", localityCode);
    }

    private static Pattern onlyDigits = Pattern.compile("^\\s*[0-9]+$");

    protected static String formatFloor(String floor) {
        if (floor == null || floor.isEmpty()) return "";
        Matcher m = onlyDigits.matcher(floor);
        if (m.find()) {
            return StringUtils.leftPad(floor, 2, '0');
        }
        return floor;
    }

    protected static String formatDoor(String door) {
        if (door == null || door.isEmpty()) return "";
        Matcher m = onlyDigits.matcher(door);
        if (m.find()) {
            return StringUtils.leftPad(door, 4, '0');
        }
        return door;
    }

    protected static String string(int value) {
        return Integer.toString(value);
    }

    protected abstract void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException;

    private static ZoneId timezone = ZoneId.systemDefault();

    protected String formatTime(OffsetDateTime time) {
        if (time == null) return "";
        return this.formatTime(time.atZoneSameInstant(timezone));
    }

    protected String formatTime(ZonedDateTime time) {
        if (time == null) return "";
        return this.formatTime(time.toLocalDate());
    }

    protected String formatTime(LocalDate time) {
        if (time == null) return "";
        return time.format(dmyFormatter);
    }

    public static LocalDate convertDate(OffsetDateTime date) {
        if (date != null) {
            return date.atZoneSameInstant(timezone).toLocalDate();
        }
        return null;
    }

    protected static void replaceMapValues(Map<String, String> map, String search, String replace) {
        for (String key : map.keySet()) {
            if (Objects.equals(map.get(key), search)) {
                map.put(key, replace);
            }
        }
    }

}
