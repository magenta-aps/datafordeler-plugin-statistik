package dk.magenta.datafordeler.statistik.reportExecution;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.hibernate.Session;
import org.hibernate.jpa.QueryHints;

import javax.persistence.TypedQuery;
import javax.persistence.criteria.CriteriaBuilder;
import javax.persistence.criteria.CriteriaQuery;
import javax.persistence.criteria.Root;

public class ReportSyncHandler {

    private Logger log = LogManager.getLogger(ReportSyncHandler.class);
    private Session session;

    public ReportSyncHandler(Session session) {
        this.session = session;
    }

    public synchronized Session getSession() {
        return this.session;
    }



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

        System.out.println(query.getResultList().size());

        if(query.getResultList().size() > 0) {
            query.getResultList().get(0).setReportStatus(status);
            session.save(query.getResultList().get(0));
        }
        session.getTransaction().commit();



    }


}
