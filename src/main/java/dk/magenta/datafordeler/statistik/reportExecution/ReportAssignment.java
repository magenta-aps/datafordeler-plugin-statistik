package dk.magenta.datafordeler.statistik.reportExecution;

import dk.magenta.datafordeler.core.database.DatabaseEntry;
import dk.magenta.datafordeler.cpr.CprPlugin;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.UpdateTimestamp;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = CprPlugin.DEBUG_TABLE_PREFIX + "cpr_report_assignment_list")
public class ReportAssignment extends DatabaseEntry {

    public static final String DB_FIELD_REPORTTEMPLATENAME = "reportTemplateName";
    public static final String DB_FIELD_REPORTUUID = "reportUuid";
    public static final String  DB_FIELD_REPORTID_STATUS = "reportIdStatus";
    public static final String  DB_FIELD_REPORTID_REASON = "reportIdReason";


    @CreationTimestamp
    private LocalDateTime createDateTime;

    @UpdateTimestamp
    private LocalDateTime updateDateTime;

    @Column(name = DB_FIELD_REPORTTEMPLATENAME, nullable = false, unique = false)
    private String reportTemplateName;


    @Column(name = DB_FIELD_REPORTUUID, nullable = false, unique = true)
    private UUID uuid;

    public ReportAssignment() {
        this.uuid = UUID.randomUUID();
    }

    public UUID getUuid() {
        return this.uuid;
    }

    @Column(name = DB_FIELD_REPORTID_REASON, length = 30, nullable = true, unique = false)
    private String reason;

    @Column(name = DB_FIELD_REPORTID_STATUS)
    @Enumerated(EnumType.ORDINAL)
    private ReportProgressStatus reportStatus;

    public String getTemplateName() {
        return this.reportTemplateName;
    }

    public void setTemplateName(String reportTemplateName) {
        this.reportTemplateName = reportTemplateName;
    }


    public String getReason() {
        return reason;
    }

    public void setReason(String reason) {
        this.reason = reason;
    }

    public ReportProgressStatus getStatus() {
        return reportStatus;
    }

    public void setStatus(ReportProgressStatus reportStatus) {
        this.reportStatus = reportStatus;
    }
}
