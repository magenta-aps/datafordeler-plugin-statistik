package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.core.io.ImportMetadata;
import dk.magenta.datafordeler.cpr.data.person.PersonEntityManager;
import org.hibernate.Session;
import org.hibernate.Transaction;

import java.io.InputStream;

public class PersonTestsUtils {
   // @Autowired
    private SessionManager sessionManager;

   // @Autowired
    private PersonEntityManager personEntityManager;


    public PersonTestsUtils(SessionManager sessionManager,
                            PersonEntityManager personEntityManager){
        this.sessionManager = sessionManager;
        this.personEntityManager = personEntityManager;
    }

    public void loadPersonData() throws Exception {
        InputStream testData = PersonTestsUtils.class.getResourceAsStream("/person.txt");
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
}
