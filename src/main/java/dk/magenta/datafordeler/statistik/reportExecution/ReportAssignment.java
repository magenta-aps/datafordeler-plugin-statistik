package dk.magenta.datafordeler.statistik.reportExecution;

import dk.magenta.datafordeler.core.database.DatabaseEntry;
import dk.magenta.datafordeler.cpr.CprPlugin;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = CprPlugin.DEBUG_TABLE_PREFIX + "report_assignment_list")
public class ReportAssignment extends DatabaseEntry {

    public static final String DB_FIELD_REPORTTEMPLATENAME = "reportTemplateName";
    public static final String DB_FIELD_REPORTUUID = "reportUuid";
    public static final String DB_FIELD_COLLECTIONUUID = "collectionUuid";
    public static final String  DB_FIELD_REPORT_STATUS = "reportStatus";
    public static final String  DB_FIELD_REPORTID_REASON = "reportIdReason";
    public static final String  DB_FIELD_REPORT_FILE_NAME = "reportFileName";
    public static final String  DB_FIELD_REGISTRATIONAFTER = "registrationAfter";
    public static final String  DB_FIELD_REGISTRATIONBEFORE = "registrationBefore";


    public ReportAssignment() {
        this.reportUuid = UUID.randomUUID().toString();
        this.collectionUuid = UUID.randomUUID().toString();
        this.reportStatus = ReportProgressStatus.started;
    }

    public ReportAssignment(String collectionUuid) {
        this.collectionUuid = collectionUuid;
        this.reportUuid = UUID.randomUUID().toString();
        this.reportStatus = ReportProgressStatus.started;
    }


    @CreationTimestamp
    private LocalDateTime createDateTime;

    @UpdateTimestamp
    private LocalDateTime updateDateTime;

    @Column(name = DB_FIELD_REPORTTEMPLATENAME, nullable = false, unique = false)
    private String reportTemplateName;

    public String getTemplateName() {
        return this.reportTemplateName;
    }

    public void setTemplateName(String reportTemplateName) {
        this.reportTemplateName = reportTemplateName;
    }


    @Column(name = DB_FIELD_REPORTUUID, nullable = false)
    private String reportUuid;

    public String getReportUuid() {
        return this.reportUuid;
    }

    @Column(name = DB_FIELD_COLLECTIONUUID, nullable = false)
    private String collectionUuid;

    public String getCollectionUuid() {
        return this.collectionUuid;
    }

    @Column(name = DB_FIELD_REGISTRATIONAFTER)
    private String registrationAfter;

    public String getRegistrationAfter() {
        return this.registrationAfter;
    }

    public void setRegistrationAfter(String registrationAfter) {
        this.registrationAfter = registrationAfter;
    }

    @Column(name = DB_FIELD_REGISTRATIONBEFORE)
    private String registrationBefore;

    public String getRegistrationBefore() {
        return this.registrationBefore;
    }

    public void setRegistrationBefore(String registrationBefore) {
        this.registrationBefore = registrationBefore;
    }

    @Column(name = DB_FIELD_REPORT_FILE_NAME, nullable = false)
    public String getFilename() {
        return this.reportTemplateName + "_" + this.reportUuid;
    }


    @Column(name = DB_FIELD_REPORTID_REASON, length = 30, nullable = true, unique = false)
    private String reason;

    @Column(name = DB_FIELD_REPORT_STATUS)
    @Enumerated(EnumType.ORDINAL)
    private ReportProgressStatus reportStatus;




    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public ReportProgressStatus getReportStatus() {
        return reportStatus;
    }

    public void setReportStatus(ReportProgressStatus reportStatus) {
        this.reportStatus = reportStatus;
    }
}
