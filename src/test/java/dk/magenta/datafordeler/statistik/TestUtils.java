package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.database.*;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.io.ImportInputStream;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.core.util.LabeledSequenceInputStream;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.geo.GeoPlugin;
import dk.magenta.datafordeler.geo.data.road.RoadEntityManager;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.junit.Assert;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.*;

import static org.mockito.Mockito.when;

@Component
public class TestUtils {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private PersonEntityManager personEntityManager;

    @Autowired
    private dk.magenta.datafordeler.cpr.data.road.RoadEntityManager cprRoadEntityManager;

    @Autowired
    private GeoPlugin geoPlugin;

    @Autowired
    private RoadEntityManager roadEntityManager;

    @Autowired
    private dk.magenta.datafordeler.geo.data.locality.LocalityEntityManager localityEntityManager;

    @Autowired
    private dk.magenta.datafordeler.geo.data.accessaddress.AccessAddressEntityManager accessAddressEntityManager;

    @Autowired
    private dk.magenta.datafordeler.geo.data.postcode.PostcodeEntityManager postcodeEntityManager;

    @SpyBean
    private DafoUserManager dafoUserManager;

    HashSet<Entity> createdEntities = new HashSet<>();

    public TestUtils(SessionManager sessionManager,
                     PersonEntityManager personEntityManager){
        this.sessionManager = sessionManager;
        this.personEntityManager = personEntityManager;
    }

    private String oldPath;
    public void setPath() throws IOException {
        //Use this code block when temp directories need to be created
        Path path = Files.createTempDirectory("statistik");
        this.oldPath = StatisticsService.PATH_FILE;
        StatisticsService.PATH_FILE = String.valueOf(path);
    }

    public void clearPath() {
        if (!Objects.equals(StatisticsService.PATH_FILE, this.oldPath)) {
            this.deleteFiles(StatisticsService.PATH_FILE);
        }
    }

    public void loadPersonData(File source) throws Exception {
        loadPersonData(new FileInputStream(source));
    }

    public void loadPersonData(String resource) throws Exception {
        loadPersonData(
                new ImportInputStream(
                        new LabeledSequenceInputStream(
                                Collections.singletonList(
                                        Pair.of(
                                                resource,
                                                TestUtils.class.getResourceAsStream("/" + resource)
                                        )
                                )
                        )
                )
        );
    }


    public void loadRoadData(String resource) throws Exception {
        loadRoadData(
                new ImportInputStream(
                        new LabeledSequenceInputStream(
                                Collections.singletonList(
                                        Pair.of(
                                                resource,
                                                TestUtils.class.getResourceAsStream("/" + resource)
                                        )
                                )
                        )
                )
        );
    }



    public void loadRoadData(InputStream testData) throws Exception {
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        cprRoadEntityManager.parseData(testData, importMetadata);
        session.close();
        testData.close();
    }

    public void loadGeoRoadData(String resource) throws DataFordelerException {
        loadGeoRoadData(
                new ImportInputStream(
                        new LabeledSequenceInputStream(
                                Collections.singletonList(
                                        Pair.of(
                                                resource,
                                                TestUtils.class.getResourceAsStream("/" + resource)
                                        )
                                )
                        )
                )
        );
    }


    public void loadGeoRoadData(InputStream testData) throws DataFordelerException {
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        Transaction transaction = session.beginTransaction();
        importMetadata.setTransactionInProgress(true);
        roadEntityManager.parseData(testData, importMetadata);
        transaction.commit();
        session.close();
    }

    public void loadGeoLocalityData(String resource) throws DataFordelerException {
        loadGeoLocalityData(
                new ImportInputStream(
                        new LabeledSequenceInputStream(
                                Collections.singletonList(
                                        Pair.of(
                                                resource,
                                                TestUtils.class.getResourceAsStream("/" + resource)
                                        )
                                )
                        )
                )
        );
    }


    public void loadGeoLocalityData(InputStream testData) throws DataFordelerException {
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        Transaction transaction = session.beginTransaction();
        importMetadata.setTransactionInProgress(true);
        localityEntityManager.parseData(testData, importMetadata);
        transaction.commit();
        session.close();
    }


    public void loadAccessLocalityData(String resource) throws DataFordelerException {
        loadAccessLocalityData(
                new ImportInputStream(
                        new LabeledSequenceInputStream(
                                Collections.singletonList(
                                        Pair.of(
                                                resource,
                                                TestUtils.class.getResourceAsStream("/" + resource)
                                        )
                                )
                        )
                )
        );
    }


    public void loadAccessLocalityData(InputStream testData) throws DataFordelerException {
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        Transaction transaction = session.beginTransaction();
        importMetadata.setTransactionInProgress(true);
        accessAddressEntityManager.parseData(testData, importMetadata);
        transaction.commit();
        session.close();
    }


    public void loadPostalLocalityData(String resource) throws DataFordelerException {
        loadPostalLocalityData(
                new ImportInputStream(
                        new LabeledSequenceInputStream(
                                Collections.singletonList(
                                        Pair.of(
                                                resource,
                                                TestUtils.class.getResourceAsStream("/" + resource)
                                        )
                                )
                        )
                )
        );
    }


    public void loadPostalLocalityData(InputStream testData) throws DataFordelerException {
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        Transaction transaction = session.beginTransaction();
        importMetadata.setTransactionInProgress(true);
        postcodeEntityManager.parseData(testData, importMetadata);
        transaction.commit();
        session.close();
    }



    public void loadPersonData(InputStream testData) throws Exception {
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        Transaction transaction = session.beginTransaction();
        importMetadata.setTransactionInProgress(true);
        personEntityManager.parseData(testData, importMetadata);
        transaction.commit();
        session.close();
    }

    public void applyAccess(TestUserDetails testUserDetails) {
        when(dafoUserManager.getFallbackUser()).thenReturn(testUserDetails);
    }

    public <E extends DatabaseEntry> void deleteAll(Class<E> eClass) {
        Session session = sessionManager.getSessionFactory().openSession();
        Transaction transaction = session.beginTransaction();
        Collection<E> entities = QueryManager.getAllEntities(session, eClass);
        for (E entity : entities) {
            session.delete(entity);
        }
        transaction.commit();
        session.close();
    }

    public void deleteAll() {
        this.deleteAll(PersonEntity.class);
    }

    public void deleteFiles(String path_file){
        //This code can be places in @After
        File tempDir = null;
        try {
            tempDir = new File(path_file);
            boolean exists = tempDir.exists();
            Assert.assertEquals(true, exists);
        } catch (Exception e) {
            // if any error occurs
            e.printStackTrace();
        } finally {
            File[] listOfFiles = tempDir.listFiles();
            if (listOfFiles.length > 0) {

                System.out.println("Number of files to delete: " + listOfFiles.length);
                for (File file : listOfFiles) {
                    file.deleteOnExit();
                    System.out.println("Deleted file: " + file.getName());
                }

            }
            try {
                FileUtils.deleteDirectory(new File(path_file));
                System.out.println("Deleted directory: " + path_file);
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
