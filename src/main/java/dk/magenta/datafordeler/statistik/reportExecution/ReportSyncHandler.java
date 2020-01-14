package dk.magenta.datafordeler.statistik.reportExecution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.jpa.QueryHints;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;
import java.util.ArrayList;
import java.util.List;

/**
 * Handler for objects that keeps track of running reports
 */
public class ReportSyncHandler {

    private Logger log = LogManager.getLogger(ReportSyncHandler.class);
    private Session session;

    public ReportSyncHandler(Session session) {
        this.session = session;
    }

    public synchronized Session getSession() {
        return this.session;
    }

    /**
     * Create a reportstatus object, reject if there is any which is not done
     * @param reportStatusObject
     * @return
     * @throws Exception
     */
    public boolean createReportStatusObject(ReportAssignment reportStatusObject) throws Exception {
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
        Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
        criteria.select(page);
        criteria.where(builder.notEqual(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.done));

        TypedQuery<ReportAssignment> query = session.createQuery(criteria);
        query.setHint(QueryHints.HINT_CACHEABLE, true);

        if (query.getResultList().size() > 0) {
            return false;
        }

        session.beginTransaction();
        session.save(reportStatusObject);
        session.getTransaction().commit();
        return true;
    }

    public boolean hasUndoneReportsStatus() {
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
        Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
        criteria.select(page);
        criteria.where(builder.notEqual(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.done));

        TypedQuery<ReportAssignment> query = session.createQuery(criteria);
        query.setHint(QueryHints.HINT_CACHEABLE, true);

        return query.getResultList().size()>0;
    }

    public boolean hasReportsOfStatus(ReportProgressStatus status) {
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
        Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
        criteria.select(page);
        criteria.where(builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), status));

        TypedQuery<ReportAssignment> query = session.createQuery(criteria);
        query.setHint(QueryHints.HINT_CACHEABLE, true);

        return query.getResultList().size()>0;
    }


    /**
     * Set a new status to a report, it is invalid to change status on a failed report
     * @param reportUuid
     * @param status
     */
    public void setReportStatus(String reportUuid, ReportProgressStatus status) {

        session.beginTransaction();

        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
        Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
        criteria.select(page);
        criteria.where(builder.and(
                builder.equal(page.get(ReportAssignment.DB_FIELD_REPORTUUID), reportUuid),
                builder.notEqual(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), ReportProgressStatus.failed)
        ));

        TypedQuery<ReportAssignment> query = session.createQuery(criteria);
        query.setHint(QueryHints.HINT_CACHEABLE, true);

        if(query.getResultList().size() > 0) {
            query.getResultList().get(0).setReportStatus(status);
            session.save(query.getResultList().get(0));
        }
        session.getTransaction().commit();
    }

    /**
     * Get a list of reports attached to a specified collectionUuid
     * @param collectionUuid
     * @param reportProgressStatus
     * @return
     */
    public List<String> getReportList(String collectionUuid, ReportProgressStatus reportProgressStatus) {

        ArrayList<String> reports = new ArrayList<String>();
        CriteriaBuilder builder = session.getCriteriaBuilder();
        CriteriaQuery<ReportAssignment> criteria = builder.createQuery(ReportAssignment.class);
        Root<ReportAssignment> page = criteria.from(ReportAssignment.class);
        criteria.select(page);
        criteria.where(builder.and(
                builder.equal(page.get(ReportAssignment.DB_FIELD_COLLECTIONUUID), collectionUuid),
                builder.equal(page.get(ReportAssignment.DB_FIELD_REPORT_STATUS), reportProgressStatus)
        ));

        TypedQuery<ReportAssignment> query = session.createQuery(criteria);

        for(ReportAssignment report : query.getResultList()) {
            reports.add(report.getTemplateName()+"_"+report.getReportUuid());
        }
        return reports;
    }
}
