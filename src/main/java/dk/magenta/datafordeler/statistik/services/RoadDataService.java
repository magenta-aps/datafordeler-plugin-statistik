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
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprPlugin;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.road.RoadRecordQuery;
import dk.magenta.datafordeler.cpr.records.road.data.RoadCityBitemporalRecord;
import dk.magenta.datafordeler.cpr.records.road.data.RoadEntity;
import dk.magenta.datafordeler.cpr.records.road.data.RoadPostalcodeBitemporalRecord;
import dk.magenta.datafordeler.statistik.utils.Filter;
import dk.magenta.datafordeler.statistik.utils.LookupService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.Part;
import java.io.*;
import java.time.OffsetDateTime;
import java.util.*;
import java.util.function.Consumer;
import java.util.regex.Pattern;
import java.util.stream.Stream;

@RestController
@RequestMapping("/statistik/road_data")
public class RoadDataService extends StatisticsService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    @Autowired
    private CprPlugin cprPlugin;

    @PostConstruct
    public void init() {
        this.setWriteToLocalFile(false);
    }


    private Logger log = LogManager.getLogger(RoadDataService.class);

    @Override
    protected DafoUserDetails getUser(HttpServletRequest request) throws InvalidTokenException, AccessDeniedException, InvalidCertificateException {
        String formToken = request.getParameter("token");
        if (formToken != null) {
            return this.getDafoUserManager().getSamlUserDetailsFromToken(formToken);
        }
        return super.getUser(request);
    }

    protected void checkAndLogAccess(LoggerHelper loggerHelper) throws AccessDeniedException, AccessRequiredException {
        try {
            loggerHelper.getUser().checkHasSystemRole(CprRolesDefinition.READ_CPR_ROLE);
        } catch (AccessDeniedException e) {
            loggerHelper.info("Access denied: " + e.getMessage());
            throw (e);
        }
    }

    @RequestMapping(method = RequestMethod.POST, path = "/")
    public void handlePost(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.ROAD);
    }


    @Override
    protected CprPlugin getCprPlugin() {
        return this.cprPlugin;
    }

    @Override
    protected List<String> getColumnNames() {
        return Arrays.asList(new String[]{
                MUNICIPALITY_CODE, LOCALITY_CODE, ROAD_CODE, ROAD_NAME, BYGDE, POST_CODE, "Void"
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
    protected List<Map<String, String>> formatPerson(PersonEntity person, Session session, LookupService lookupService, Filter filter) {
        return null;
    }

    private static Pattern numeric = Pattern.compile("\\d+");
    private static int limit = 1000;



    public int run(Filter filter, OutputStream outputStream) {

        final Session primarySession = this.getSessionManager().getSessionFactory().openSession();
        final Session secondarySession = this.getSessionManager().getSessionFactory().openSession();

        try {

            primarySession.setDefaultReadOnly(true);
            secondarySession.setDefaultReadOnly(true);

            RoadRecordQuery query = new RoadRecordQuery();

            List<RoadEntity> entities = QueryManager.getAllEntities(primarySession, query, RoadEntity.class);
            Stream<Map<String, String>> concatenation = null;

            for (RoadEntity entity : entities) {
                Stream<Map<String, String>> formatted = this.formatRoad(entity).stream();
                concatenation = (concatenation == null) ? formatted : Stream.concat(concatenation, formatted);
            }

            if (concatenation != null) {
                if (outputStream != null) {
                    return this.writeItems(concatenation.iterator(), outputStream, item -> {

                    });
                }
            }

        } catch (Exception e) {
            log.error("Exception", e);
        } finally {
            primarySession.close();
            secondarySession.close();
        }
        return 0;
    }




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



    private List<Map<String, String>> formatRoad(dk.magenta.datafordeler.cpr.records.road.data.RoadEntity road) {

        String munitpialityCode = road.getMunicipalityCode()+"";
        String roadCode = road.getRoadcode()+"";
        String roadName = road.getName().size()>0 ? (road.getName().stream().findFirst().get().getRoadName()+"") : "";

        List<Map<String, String>> list = new ArrayList<Map<String, String>>();
        HashMap<String, String> item = new HashMap<>();

        Iterator<RoadPostalcodeBitemporalRecord> roadPostalcodes = road.getPostcode().iterator();
        while(roadPostalcodes.hasNext()) {
            RoadPostalcodeBitemporalRecord postalCode = roadPostalcodes.next();
            item.put(MUNICIPALITY_CODE, munitpialityCode);
            item.put(LOCALITY_CODE, "");
            item.put(ROAD_CODE, roadCode);
            item.put(ROAD_NAME, roadName);

            //new roadcode is expected to always mean new city, that is why there is only one
            String cityName = road.getCity().size()>0 ? (road.getCity().stream().findFirst().get().getCityName()+"") : "";
            item.put(BYGDE, cityName);

            item.put(POST_CODE, postalCode.getPostalCode()+"");
            item.put("Void", (road.getName().size()>1 ? (road.getName().size()+"") : ""));
            list.add(item);
        }

        return list;
    }

    @Override
    protected Filter getFilter(HttpServletRequest request) {
        Filter filter = new Filter();

        InputStream inputStream = null;
        String fieldName = "file"; // Matches the file field in addressServiceForm.html
        try {
            Part filePart = request.getPart(fieldName);
            if (filePart != null) {
                inputStream = filePart.getInputStream();
            }
        } catch (ServletException | IOException e) {
            e.printStackTrace();
        }
        if (inputStream != null) {

            ArrayList<String> pnrs = new ArrayList<>();
            Stream<String> stream = new BufferedReader(new InputStreamReader(inputStream)).lines();

            try {
                stream.forEach(pnrs::add);
            } finally {
                stream.close();
                try {
                    inputStream.close();
                } catch (IOException e) {
                    log.error("IOException", e);
                }
            }
            filter.onlyPnr = pnrs;
        }

        filter.effectAt = OffsetDateTime.now();
        return filter;
    }
}
