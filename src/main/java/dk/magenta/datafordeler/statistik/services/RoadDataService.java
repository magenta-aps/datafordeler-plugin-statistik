package dk.magenta.datafordeler.statistik.services;


import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectWriter;
import com.fasterxml.jackson.databind.SequenceWriter;
import com.fasterxml.jackson.dataformat.csv.CsvGenerator;
import com.fasterxml.jackson.dataformat.csv.CsvMapper;
import com.fasterxml.jackson.dataformat.csv.CsvSchema;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.*;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LoggerHelper;
import dk.magenta.datafordeler.cpr.CprRolesDefinition;
import dk.magenta.datafordeler.geo.data.accessaddress.AccessAddressEntity;
import dk.magenta.datafordeler.geo.data.accessaddress.AccessAddressRoadRecord;
import dk.magenta.datafordeler.geo.data.locality.GeoLocalityEntity;
import dk.magenta.datafordeler.geo.data.road.GeoRoadEntity;
import dk.magenta.datafordeler.statistik.StatistikRolesDefinition;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.PostConstruct;
import org.hibernate.query.Query;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.io.*;
import java.util.*;
import java.util.function.Consumer;

/**
 *  This might be a naive implementation, but so far it does not look like it
 *  It does not take bitemporality or noe to many relations on roadentity into account
 */
@RestController
@RequestMapping("/statistik/road_data")
public class RoadDataService extends StatisticsService {

    @Autowired
    SessionManager sessionManager;

    @Autowired
    private CsvMapper csvMapper;

    @Autowired
    private DafoUserManager dafoUserManager;

    @PostConstruct
    public void init() {
        this.setWriteToLocalFile(true);
    }


    private Logger log = LogManager.getLogger(RoadDataService.class);

    @RequestMapping(method = RequestMethod.POST, path = "/")
    public void handlePost(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, IOException, MissingParameterException, InvalidClientInputException, HttpNotFoundException, InvalidCertificateException {
        super.handleRequest(request, response, ServiceName.ROAD);
    }


    @RequestMapping(method = RequestMethod.GET, path = "/")
    public void get(HttpServletRequest request, HttpServletResponse response)
            throws AccessDeniedException, AccessRequiredException, InvalidTokenException, InvalidClientInputException, IOException, HttpNotFoundException, MissingParameterException, InvalidCertificateException {
        log.info("Service called");
        super.handleRequest(request, response, ServiceName.ROAD);
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

    public int run(Filter filter, OutputStream outputStream, String reportUuid) {

        final Session primarySession = this.getSessionManager().getSessionFactory().openSession();
        final Session secondarySession = this.getSessionManager().getSessionFactory().openSession();

        try {

            primarySession.setDefaultReadOnly(true);
            secondarySession.setDefaultReadOnly(true);

            List<Map<String, String>> concatenation = new ArrayList<>();

            String hql = "SELECT DISTINCT roadEntity,roadMunicipality.code,accessAddressPostcode.postcode ,roadName.name, locality " +
                    "FROM "+GeoRoadEntity.class.getCanonicalName()+" roadEntity "+
                    "JOIN roadEntity."+GeoRoadEntity.DB_FIELD_IDENTIFICATION+" roadIdentification "+
                    "LEFT JOIN "+ AccessAddressRoadRecord.class.getCanonicalName()+" accessAddressRoad ON accessAddressRoad.reference = roadIdentification " +
                    "JOIN accessAddressRoad."+AccessAddressRoadRecord.DB_FIELD_ENTITY+" accessAddress "+
                    "LEFT JOIN accessAddress."+AccessAddressEntity.DB_FIELD_POSTCODE+" accessAddressPostcode "+
                    "JOIN roadEntity."+GeoRoadEntity.DB_FIELD_MUNICIPALITY+" roadMunicipality " +
                    "JOIN roadEntity."+GeoRoadEntity.DB_FIELD_NAME+" roadName " +
                    "LEFT JOIN accessAddress."+AccessAddressEntity.DB_FIELD_POSTCODE+" accessAddressPostcode "+
                    "JOIN roadEntity."+GeoRoadEntity.DB_FIELD_LOCALITY+" roadLocality " +
                    "JOIN " + GeoLocalityEntity.class.getCanonicalName()+" locality ON roadLocality.reference = locality."+GeoLocalityEntity.DB_FIELD_IDENTIFICATION+" "+
                    "JOIN locality."+GeoLocalityEntity.DB_FIELD_NAME+" localityName" +
                    "";

            Query query = secondarySession.createQuery(hql);
            for (Object item : query.getResultList()) {
                Object[] items = (Object[]) item;
                GeoRoadEntity roadEntity = (GeoRoadEntity) items[0];
                Integer municipalityCode = (Integer) items[1];
                Integer postcode = (Integer) items[2];
                String roadName = (String) items[3];
                GeoLocalityEntity locality = (GeoLocalityEntity) items[4];
                HashMap<String, String> csvRow = new HashMap<>();
                csvRow.put(MUNICIPALITY_CODE, municipalityCode+"");
                csvRow.put(LOCALITY_CODE, locality.getCode()+"");
                csvRow.put(ROAD_CODE, roadEntity.getCode()+"");
                csvRow.put(ROAD_NAME, roadName);
                //It is expected that there is only one name on a roadentity
                csvRow.put(BYGDE, locality.getName().iterator().next().getName());
                csvRow.put(POST_CODE, postcode+"");
                concatenation.add(csvRow);
            }


            if (outputStream != null) {
                return this.writeItems(concatenation.iterator(), outputStream, item -> {

                });
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

}
