package dk.magenta.datafordeler.statistik.reportExecution;

import dk.magenta.datafordeler.core.database.SessionManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.springframework.beans.factory.annotation.Autowired;

public class ReportSync {

    private Logger log = LogManager.getLogger(ReportSync.class);

    private Session session;

    public ReportSync(Session session) {
        this.session = session;
    }

    public synchronized Session getSession() {
        return this.session;
    }


    ReportAssignment reportAssignments;

    public String setReportProgressObject(ReportAssignment reportAssignments) {
        this.reportAssignments = reportAssignments;
        this.log.info("Collected these numbers for subscription: "+reportAssignments);
        session.beginTransaction();
        session.save(reportAssignments);
        String uuid = reportAssignments.getUuid().toString();
        session.getTransaction().commit();
        return uuid;
    }


    /**
     * Create subscriptions by adding them to the table of subscriptions
     * @param reportStatus
     */
    public void setReportStatus(ReportProgressStatus reportStatus) {
        this.log.info("Collected these numbers for subscription: "+reportAssignments);
        session.beginTransaction();
        reportAssignments.setStatus(reportStatus);
        session.update(reportAssignments);
        String uuid = reportAssignments.getUuid().toString();
        session.getTransaction().commit();

    }

}
