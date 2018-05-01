package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.database.Entity;
import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.exception.DataFordelerException;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.core.user.DafoUserManager;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import dk.magenta.datafordeler.cpr.data.person.PersonRegistration;
import dk.magenta.datafordeler.cvr.data.unversioned.Municipality;
import dk.magenta.datafordeler.gladdrreg.GladdrregPlugin;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.locality.LocalityRegistration;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntity;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.municipality.MunicipalityRegistration;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntity;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.postalcode.PostalCodeRegistration;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntity;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadEntityManager;
import dk.magenta.datafordeler.gladdrreg.data.road.RoadRegistration;
import org.hibernate.Session;
import org.hibernate.Transaction;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.mock.mockito.SpyBean;
import org.springframework.stereotype.Component;

import java.io.IOException;
import java.io.InputStream;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;

import static org.mockito.Mockito.when;

@Component
public class PersonTestsUtils {

    @Autowired
    private SessionManager sessionManager;

    @Autowired
    private PersonEntityManager personEntityManager;

    @Autowired
    private GladdrregPlugin gladdrregPlugin;

    @SpyBean
    private DafoUserManager dafoUserManager;

    HashSet<Entity> createdEntities = new HashSet<>();

    public PersonTestsUtils(SessionManager sessionManager,
                            PersonEntityManager personEntityManager){
        this.sessionManager = sessionManager;
        this.personEntityManager = personEntityManager;
    }

    public void loadPersonData(String resource) throws Exception {
        InputStream testData = PersonTestsUtils.class.getResourceAsStream("/" + resource);
        ImportMetadata importMetadata = new ImportMetadata();
        Session session = sessionManager.getSessionFactory().openSession();
        importMetadata.setSession(session);
        Transaction transaction = session.beginTransaction();
        importMetadata.setTransactionInProgress(true);
        personEntityManager.parseData(testData, importMetadata);
        transaction.commit();
        session.close();
        testData.close();
    }

    private void loadLocality(Session session) throws DataFordelerException, IOException {
        List<? extends Registration> regs;
        try (InputStream testData = PersonTestsUtils.class.getResourceAsStream("/locality.json")) {
            LocalityEntityManager localityEntityManager = (LocalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(LocalityEntity.schema);
            regs = localityEntityManager.parseData(testData, new ImportMetadata());
            testData.close();
        }
        for (Registration registration : regs) {
            LocalityRegistration localityRegistration = (LocalityRegistration) registration;
            QueryManager.saveRegistration(session, localityRegistration.getEntity(), localityRegistration);
            createdEntities.add(localityRegistration.getEntity());
        }
    }

    private void loadRoad(Session session) throws DataFordelerException, IOException {
        InputStream testData = PersonTestsUtils.class.getResourceAsStream("/road.json");
        RoadEntityManager roadEntityManager = (RoadEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(RoadEntity.schema);
        List<? extends Registration> regs = roadEntityManager.parseData(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            RoadRegistration roadRegistration = (RoadRegistration) registration;
            QueryManager.saveRegistration(session, roadRegistration.getEntity(), roadRegistration);
            createdEntities.add(roadRegistration.getEntity());
        }
    }

    private void loadMunicipality(Session session) throws DataFordelerException, IOException {
        InputStream testData = PersonTestsUtils.class.getResourceAsStream("/municipality.json");
        MunicipalityEntityManager municipalityEntityManager = (MunicipalityEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(MunicipalityEntity.schema);
        List<? extends Registration> regs = municipalityEntityManager.parseData(testData, new ImportMetadata());
        testData.close();
        for (Registration registration : regs) {
            MunicipalityRegistration municipalityRegistration = (MunicipalityRegistration) registration;
            QueryManager.saveRegistration(session, municipalityRegistration.getEntity(), municipalityRegistration);
            createdEntities.add(municipalityRegistration.getEntity());
        }
    }

    private void loadPostalCode(Session session) throws DataFordelerException {
        InputStream testData = PersonTestsUtils.class.getResourceAsStream("/postalcode.json");
        PostalCodeEntityManager postalCodeEntityManager = (PostalCodeEntityManager) gladdrregPlugin.getRegisterManager().getEntityManager(PostalCodeEntity.schema);
        List<? extends Registration> regs = postalCodeEntityManager.parseData(testData, new ImportMetadata());
        for (Registration registration : regs) {
            PostalCodeRegistration postalCodeRegistration = (PostalCodeRegistration) registration;
            QueryManager.saveRegistration(session, postalCodeRegistration.getEntity(), postalCodeRegistration);
            createdEntities.add(postalCodeRegistration.getEntity());
        }
    }

    public void loadGladdrregData() {
        Session session = sessionManager.getSessionFactory().openSession();
        try {
            Transaction transaction = session.beginTransaction();
            loadLocality(session);
            loadRoad(session);
            loadMunicipality(session);
            loadPostalCode(session);
            transaction.commit();
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            session.close();
        }
    }

    public void applyAccess(TestUserDetails testUserDetails) {
        when(dafoUserManager.getFallbackUser()).thenReturn(testUserDetails);
    }

    public <E extends Entity> void deleteAll(Class<E> eClass) {
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
        this.deleteAll(LocalityEntity.class);
        this.deleteAll(RoadEntity.class);
        this.deleteAll(MunicipalityEntity.class);
        this.deleteAll(PostalCodeEntity.class);
    }
}
