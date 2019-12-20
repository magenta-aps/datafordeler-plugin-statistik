package dk.magenta.datafordeler.statistik.reportExecution;

import dk.magenta.datafordeler.core.database.QueryManager;
import dk.magenta.datafordeler.core.database.SessionManager;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.statistik.utils.Filter;
import org.hibernate.Session;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Stream;

public class ReportRunner implements Runnable {

    private int counter;
    SessionManager sessionManager;

    // Constructor
    public ReportRunner(int counter, SessionManager sessionManager) {
        this.counter = counter;
        this.sessionManager = sessionManager;
    }

    @Override
    public void run() {

        try(final Session primarySession = this.sessionManager.getSessionFactory().openSession();
            final Session secondarySession = this.sessionManager.getSessionFactory().openSession();) {

            primarySession.setDefaultReadOnly(true);
            secondarySession.setDefaultReadOnly(true);

            Filter f = new Filter();

            List<PersonRecordQuery> queries = this.getQueryList(new Filter());
            Stream<Map<String, String>> concatenation = null;

            for (PersonRecordQuery query : queries) {
                Stream<PersonEntity> personEntities = QueryManager.getAllEntitiesAsStream(primarySession, query, PersonEntity.class);

                System.out.println(personEntities.count());

            }


        } catch (Exception e) {
            //log.error("Failed generating report", e);
            //repSync.setReportStatus(ReportProgressStatus.failed);
        } finally {
            //log.info("Done writing report");
            //repSync.setReportStatus(ReportProgressStatus.done);
        }





        while(counter>0) {
            System.out.println("RUN");
            counter--;
            try {
                Thread.sleep(5000);
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }
    }




    protected List<PersonRecordQuery> getQueryList(Filter filter) throws IOException {
        return Collections.singletonList(this.getQuery(filter));
    }

    protected PersonRecordQuery getQuery(Filter filter) {
        PersonRecordQuery personQuery = new PersonRecordQuery();
        if (filter.onlyPnr != null) {
            for (String pnr : filter.onlyPnr) {
                personQuery.addPersonnummer(pnr);
            }
        }
        return personQuery;
    }



}
