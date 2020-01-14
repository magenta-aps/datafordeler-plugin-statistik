package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.database.DatabaseEntry;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.geo.data.GeoEntityManager;
import dk.magenta.datafordeler.geo.data.accessaddress.AccessAddressEntityManager;
import dk.magenta.datafordeler.geo.data.building.BuildingEntityManager;
import dk.magenta.datafordeler.geo.data.locality.LocalityEntityManager;
import dk.magenta.datafordeler.geo.data.municipality.MunicipalityEntityManager;
import dk.magenta.datafordeler.geo.data.postcode.PostcodeEntityManager;
import dk.magenta.datafordeler.geo.data.road.RoadEntityManager;
import dk.magenta.datafordeler.geo.data.unitaddress.UnitAddressEntityManager;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import java.io.IOException;
import java.io.InputStream;
import java.util.List;

public abstract class TestBase {


    @Autowired
    private LocalityEntityManager localityEntityManager;

    @Autowired
    private RoadEntityManager roadEntityManager;

    @Autowired
    private BuildingEntityManager buildingEntityManager;

    @Autowired
    private MunicipalityEntityManager municipalityEntityManager;

    @Autowired
    private PostcodeEntityManager postcodeEntityManager;

    @Autowired
    private AccessAddressEntityManager accessAddressEntityManager;

    @Autowired
    private UnitAddressEntityManager unitAddressEntityManager;


    protected void loadAllGeoAdress(SessionManager sessionManager) throws IOException {
        this.loadGeoData(sessionManager, localityEntityManager, "/locality.json");
        this.loadGeoData(sessionManager, roadEntityManager,"/road.json");
        this.loadGeoData(sessionManager, unitAddressEntityManager, "/unit.json");
        this.loadGeoData(sessionManager, municipalityEntityManager, "/municipality.json");
        this.loadGeoData(sessionManager, postcodeEntityManager, "/post.json");
        this.loadGeoData(sessionManager, buildingEntityManager, "/building.json");
        this.loadGeoData(sessionManager, accessAddressEntityManager, "/access.json");
    }



    protected void cleanup(SessionManager sessionManager, Class[] classes) {
        Session session = sessionManager.getSessionFactory().openSession();
        Transaction transaction = session.beginTransaction();
        try {
            for (Class cls : classes) {
                List<DatabaseEntry> eList = QueryManager.getAllItems(session, cls);
                for (DatabaseEntry e : eList) {
                    session.delete(e);
                }
            }
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();

        } finally {
            session.close();
        }
    }

    protected void loadGeoData(SessionManager sessionManager, GeoEntityManager entityManager, String resourceName) throws IOException {
        InputStream data = TestBase.class.getResourceAsStream(resourceName);
        Session session = sessionManager.getSessionFactory().openSession();
        Transaction transaction = session.beginTransaction();
        ImportMetadata importMetadata = new ImportMetadata();
        try {
            importMetadata.setTransactionInProgress(true);
            importMetadata.setSession(session);
            entityManager.parseData(data, importMetadata);
            transaction.commit();
        } catch (Exception e) {
            transaction.rollback();
            e.printStackTrace();
        } finally {
            importMetadata.setTransactionInProgress(false);
            session.close();
            data.close();
        }
    }

    protected void cleanupPersonData(SessionManager sessionManager) {
        this.cleanup(sessionManager, new Class[] {
                PersonEntity.class,
        });
        QueryManager.clearCaches();
    }
}
