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
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.statistik.StatistikRolesDefinition;
import dk.magenta.datafordeler.statistik.reportExecution.ReportAssignment;
import dk.magenta.datafordeler.statistik.reportExecution.ReportProgressStatus;
import dk.magenta.datafordeler.statistik.reportExecution.ReportSyncHandler;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.time.*;
import java.time.format.DateTimeFormatter;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public abstract class StatisticsService {

    public static final ZoneId cprDataOffset = ZoneId.of("Europe/Copenhagen");

    public static String PATH_FILE = null;

    @Value("${dafo.statistics.enabled}")
    protected boolean statisticsEnabled = false;

    @Autowired
    SessionManager sessionManager;

    private Logger log = LogManager.getLogger(StatisticsService.class);

    static {
        StatisticsService.PATH_FILE = "statistik";
        File folder = new File(StatisticsService.PATH_FILE);
        if (!folder.exists()) {
            folder.mkdirs();
        }
    }

    protected String[] requiredParameters() {
        return new String[]{};
    }

    /**
     * If the request is a get-request we look for the Header "Authorization" to fund the user-rights
     * If the request is a Post-request we look for the parameter "token" to fund the user-rights
     * @param request
     * @return
     * @throws InvalidTokenException
     * @throws AccessDeniedException
     * @throws InvalidCertificateException
     */
    protected DafoUserDetails getUser(HttpServletRequest request) throws InvalidTokenException, AccessDeniedException, InvalidCertificateException {
        boolean isPost = "POST".equals(request.getMethod());
        if(isPost) {
            String formToken = request.getParameter("token");
            if (formToken != null) {
                return this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);
            }
        } else {
            return this.getDafoUserManager().getUserFromRequest(request);
        }
        return null;
    }


    public abstract int run(Filter filter, OutputStream outputStream, String reportUuid);


    /**
     * Get is used for either returning a frontpage, og starting the generation of a report
     *
     * @param request
     * @param response
     * @param serviceName
     * @throws AccessDeniedException
     * @throws AccessRequiredException
     * @throws InvalidTokenException
     * @throws IOException
     * @throws MissingParameterException
     * @throws InvalidClientInputException
     * @throws HttpNotFoundException
     * @throws InvalidCertificateException
     */
    protected void handleRequest(HttpServletRequest request, HttpServletResponse response, ServiceName serviceName) throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {
        DafoUserDetails user = this.getDafoUserManager().getUserFromRequest(request);
        if(user.isAnonymous() && request.getParameter("token")!=null) {
            String formToken = request.getParameter("token");
            if (formToken != null) {
                user = this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);
            }
        } else {
            //If the showfrontpage flag is set, only show that, login is not nessesary for that
            String showfrontpage= request.getParameter("showfrontpage");
            if(Boolean.parseBoolean(showfrontpage)) {
                IOUtils.copy(
                        StatisticsService.class.getResourceAsStream("/generalServiceForm.html"),
                        response.getWriter(), StandardCharsets.UTF_8
                );
                return;
            }
        }

        // Check that the user has access to CPR data
        //DafoUserDetails user = this.getUser(request);
        LoggerHelper loggerHelper = new LoggerHelper(this.getLogger(), request, user);
        loggerHelper.info("Incoming request for " + this.getClass().getSimpleName() + " with parameters " + request.getParameterMap());
        this.checkAndLogAccess(loggerHelper);
        for (String required : this.requiredParameters()) {
            this.requireParameter(required, request.getParameter(required));
        }

        try(Session reportProgressSession = sessionManager.getSessionFactory().openSession()) {
            Filter filter = this.getFilter(request);
            String outputDescription = null;
            OutputStream outputStream = null;

            String reportUuid = request.getParameter("reportUuid");
            String collectionUuid = request.getParameter("collectionUuid");
            ReportSyncHandler rps = new ReportSyncHandler(reportProgressSession);
            if(reportUuid==null) {
                ReportAssignment report = new ReportAssignment();
                String registrationAfter = request.getParameter("registrationAfter");
                String registrationBefore = request.getParameter("registrationBefore");
                report.setRegistrationAfter(registrationAfter);
                report.setRegistrationBefore(registrationBefore);
                report.setTemplateName(serviceName.getIdentifier());
                if(!rps.createReportStatusObject(report)) {
                    response.setStatus(HttpServletResponse.SC_CONFLICT);
                    response.getWriter().print("Execution of this report is rejected, another report is currently getting generated");
                    return;
                }
                reportUuid = report.getReportUuid();
                collectionUuid = report.getCollectionUuid();
            }


            if (this.getWriteToLocalFile()) {
                response.getWriter().print("collectionUuid: "+collectionUuid);

                if (PATH_FILE != null) {
                    File file = new File(PATH_FILE, serviceName.getIdentifier()+"_"+reportUuid + ".csv");
                    file.createNewFile();
                    outputStream = new FileOutputStream(file);
                    outputDescription = "Written to file " + file.getCanonicalPath();
                    rps.setReportStatus(reportUuid, ReportProgressStatus.done);
                }

            } else {
                response.setContentType("text/csv");
                response.setHeader("Content-Disposition", "attachment; filename=\"response.csv\"");
                outputStream = response.getOutputStream();
                outputDescription = "Written to response";
            }

            if (outputStream != null) {
                int written = this.run(filter, outputStream, reportUuid);
                this.getLogger().info(outputDescription);
                if (written == 0) {
                    response.sendError(HttpStatus.NO_CONTENT.value());
                }
            }

        } catch (Exception e) {
            loggerHelper.error("Failed creating report", e);
        }

    }

    protected abstract List<String> getColumnNames();

    protected abstract SessionManager getSessionManager();

    protected abstract CsvMapper getCsvMapper();

    protected abstract DafoUserManager getDafoUserManager();

    protected abstract Logger getLogger();

    protected Filter getFilter(HttpServletRequest request) throws Exception {
        return new Filter(request, timeintervallimit);
    }


    public enum ServiceName {
        BIRTH("birth_data"),
        DEATH("death_data"),
        CIVILSTATUS("civilstate_data"),
        MOVEMENT("movement_data"),
        STATUS("status_data"),
        ADDRESS("address_data"),
        ROAD("road_data"),
        LOCALITY("locality_data"),
        ADOPTION("adoption_data"),
        COLLECTIVE("collective_data");

        private final String identifier;

        ServiceName(String identifier) {
            this.identifier = identifier;
        }

        public String getIdentifier() {
            return this.identifier;
        }

        public static Optional<ServiceName> fromText(String text) {
            return Arrays.stream(values())
                    .filter(bl -> bl.identifier.equalsIgnoreCase(text))
                    .findFirst();
        }
    }

    private boolean writeToLocalFile = true;

    public void setWriteToLocalFile(boolean writeToLocalFile) {
        this.writeToLocalFile = writeToLocalFile;
    }

    protected boolean timeintervallimit = true;

    public void setUseTimeintervallimit(boolean timeintervallimit) {
        this.timeintervallimit = timeintervallimit;
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
    public static final String AUTHORITY_CODE_TEXT = "MynKodTxt";
    public static final String FIRST_NAME = "Fornavn";
    public static final String MIDDLE_NAME = "Mellemnavn";
    public static final String LAST_NAME = "Efternavn";
    public static final String EFFECTIVE_PNR = "PnrGaeld";
    public static final String STATUS_CODE = "Status";
    public static final String CITIZENSHIP_CODE = "StatKod";
    public static final String CIVIL_STATUS = "CivSt";
    public static final String EVENT_NAME = "Event";
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

    public static final String ADOPTIONDTO = "AdoptionDto";
    public static final String AM_mynkod = "AM_mynkod";
    public static final String AF_mynkod = "AF_mynkod";


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

    protected static String formatLocalityCode(String localityCode) {
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

    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        if(!statisticsEnabled) {
            throw new AccessDeniedException("Statistics is disabled on the server");
        }
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
            loggerHelper.getUser().checkHasSystemRole(StatistikRolesDefinition.EXECUTE_STATISTIK_ROLE);
        } catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw (e);
        }
    }

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
