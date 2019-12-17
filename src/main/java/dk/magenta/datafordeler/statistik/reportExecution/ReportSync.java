package dk.magenta.datafordeler.statistik.reportExecution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.jpa.QueryHints;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.*;

public class ReportSync {

    private Logger log = LogManager.getLogger(ReportSync.class);
    private Session session;
    private ReportAssignment reportAssignments;

    public ReportSync(Session session) {
        this.session = session;
    }

    public synchronized Session getSession() {
        return this.session;
    }


    public String setReportProgressObject(ReportAssignment reportAssignments) {
        this.reportAssignments = reportAssignments;
        this.log.info("Starting progress of reportsync: "+reportAssignments);

        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
        Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
        criteria.select(page);
        criteria.where(builder.and(
                builder.equal(page.get(ReportAssignment.DB_FIELD_REPORTTEMPLATENAME), reportAssignments.getTemplateName()),
                builder.notEqual(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.done)
        ));

        TypedQuery<ReportAssignment> query = session.createQuery(criteria);
        query.setHint(QueryHints.HINT_CACHEABLE, true);

        if(query.getResultList().size() > 0) {
            return null;
        }

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
        reportAssignments.setReportStatus(reportStatus);
        session.update(reportAssignments);
        String uuid = reportAssignments.getUuid().toString();
        session.getTransaction().commit();

    }
}
