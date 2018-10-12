package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.cpr.records.CprBitemporalRecord;
import dk.magenta.datafordeler.statistik.services.StatisticsService;
import dk.magenta.datafordeler.statistik.utils.Filter;

import javax.servlet.http.HttpServletRequest;
import java.time.LocalDate;
import java.time.OffsetDateTime;

public class PersonStatisticsQuery extends PersonRecordQuery {

    public PersonStatisticsQuery(HttpServletRequest request) {
        this.setRegistrationTimeBefore(Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_BEFORE)));
        this.setRegistrationTimeAfter(Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_AFTER)));
        this.setEffectTimeBefore(Query.parseDateTime(request.getParameter(StatisticsService.BEFORE_DATE_PARAMETER)));
        this.setEffectTimeAfter(Query.parseDateTime(request.getParameter(StatisticsService.AFTER_DATE_PARAMETER)));
        this.setOriginTimeBefore(LocalDate.parse(request.getParameter(StatisticsService.ORIGIN_BEFORE)));
        this.setOriginTimeAfter(LocalDate.parse(request.getParameter(StatisticsService.ORIGIN_AFTER)));
        String pnr = request.getParameter("pnr");
        if (pnr != null) {
            this.setPersonnummer(pnr);
        }
        this.setPageSize(1000000);
    }


    public PersonStatisticsQuery(Filter filter) {
        this.setRegistrationTimeBefore(filter.registrationBefore);
        this.setRegistrationTimeAfter(filter.registrationAfter);
        this.setEffectTimeBefore(filter.before);
        this.setEffectTimeAfter(filter.after);
        this.setOriginTimeBefore(filter.originBefore);
        this.setOriginTimeAfter(filter.originAfter);
        if (filter.onlyPnr != null) {
            for (String pnr : filter.onlyPnr) {
                this.addPersonnummer(pnr);
            }
        }
        this.setPageSize(1000000);
    }



    private LocalDate originTimeAfter = null;

    public LocalDate getOriginTimeAfter() {
        return this.originTimeAfter;
    }

    public void setOriginTimeAfter(LocalDate originTimeAfter) {
        this.originTimeAfter = originTimeAfter;
    }



    private LocalDate originTimeBefore = null;

    public LocalDate getOriginTimeBefore() {
        return this.originTimeBefore;
    }

    public void setOriginTimeBefore(LocalDate originTimeBefore) {
        this.originTimeBefore = originTimeBefore;
    }



    private OffsetDateTime registrationTimeAfter = null;

    public OffsetDateTime getRegistrationTimeAfter() {
        return this.registrationTimeAfter;
    }

    public void setRegistrationTimeAfter(OffsetDateTime registrationTimeAfter) {
        this.registrationTimeAfter = registrationTimeAfter;
    }



    private OffsetDateTime registrationTimeBefore = null;

    public OffsetDateTime getRegistrationTimeBefore() {
        return this.registrationTimeBefore;
    }

    public void setRegistrationTimeBefore(OffsetDateTime registrationTimeBefore) {
        this.registrationTimeBefore = registrationTimeBefore;
    }



    private OffsetDateTime effectTimeAfter = null;

    public OffsetDateTime getEffectTimeAfter() {
        return this.effectTimeAfter;
    }

    public void setEffectTimeAfter(OffsetDateTime effectTimeAfter) {
        this.effectTimeAfter = effectTimeAfter;
    }



    private OffsetDateTime effectTimeBefore = null;

    public OffsetDateTime getEffectTimeBefore() {
        return this.effectTimeBefore;
    }

    public void setEffectTimeBefore(OffsetDateTime effectTimeBefore) {
        this.effectTimeBefore = effectTimeBefore;
    }

    public static String cutPath(String path) {
        int firstSepIndex = path.indexOf(LookupDefinition.separator);
        return (firstSepIndex == -1) ? path : path.substring(0, firstSepIndex);
    }


    protected void applyOriginTimes(FieldDefinition fieldDefinition) {
        // Omtænk denne
        String path = cutPath(fieldDefinition.path);
        String originTimePath = path + LookupDefinition.separator + CprBitemporalRecord.DB_FIELD_ORIGIN_DATE;
        if (this.getOriginTimeAfter() != null) {
            fieldDefinition.and(
                    originTimePath,
                    this.getOriginTimeAfter(),
                    LocalDate.class,
                    LookupDefinition.Operator.GTE
            );
        }
        if (this.getOriginTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    originTimePath,
                    this.getOriginTimeBefore(),
                    LocalDate.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getOriginTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        originTimePath,
                        null,
                        LocalDate.class
                ));
            }
            fieldDefinition.and(beforeDefinition);
        }
    }

    protected FieldDefinition applyOriginTimes(String basePath) {
        // Omtænk denne
        String originTimePath = basePath + LookupDefinition.separator + CprBitemporalRecord.DB_FIELD_ORIGIN_DATE;
        FieldDefinition fieldDefinition = null;
        if (this.getOriginTimeAfter() != null) {
            FieldDefinition afterDefinition = new FieldDefinition(
                    originTimePath,
                    this.getOriginTimeAfter(),
                    LocalDate.class,
                    LookupDefinition.Operator.GTE
            );
            fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, afterDefinition);
        }
        if (this.getOriginTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    originTimePath,
                    this.getOriginTimeBefore(),
                    LocalDate.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getOriginTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        originTimePath,
                        null,
                        LocalDate.class
                ));
            }
            fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, beforeDefinition);
        }
        return fieldDefinition;
    }


    protected void applyRegistrationTimes(FieldDefinition fieldDefinition) {
        // Omtænk denne
        String path = cutPath(fieldDefinition.path);
        String registrationTimePath = path + LookupDefinition.separator + CprBitemporalRecord.DB_FIELD_REGISTRATION_FROM;
        if (this.getRegistrationTimeAfter() != null) {
            fieldDefinition.and(
                    registrationTimePath,
                    this.getRegistrationTimeAfter(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.GTE
            );
        }
        if (this.getRegistrationTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    registrationTimePath,
                    this.getRegistrationTimeBefore(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getRegistrationTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        registrationTimePath,
                        null,
                        OffsetDateTime.class
                ));
            }
            fieldDefinition.and(beforeDefinition);
        }
    }

    protected FieldDefinition applyRegistrationTimes(String basePath) {
        // Omtænk denne
        String registrationTimePath = basePath + LookupDefinition.separator + CprBitemporalRecord.DB_FIELD_REGISTRATION_FROM;
        FieldDefinition fieldDefinition = null;
        if (this.getRegistrationTimeAfter() != null) {
            FieldDefinition afterDefinition = new FieldDefinition(
                    registrationTimePath,
                    this.getRegistrationTimeAfter(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.GTE
            );
            fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, afterDefinition);
        }
        if (this.getRegistrationTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    registrationTimePath,
                    this.getRegistrationTimeBefore(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getRegistrationTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        registrationTimePath,
                        null,
                        OffsetDateTime.class
                ));
            }
            fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, beforeDefinition);
        }
        return fieldDefinition;
    }


    protected void applyEffectTimes(FieldDefinition fieldDefinition) {
        String path = cutPath(fieldDefinition.path);
        String effectTimePath = path + LookupDefinition.separator + CprBitemporalRecord.DB_FIELD_EFFECT_FROM;
        if (this.getEffectTimeAfter() != null) {
            fieldDefinition.and(
                    effectTimePath,
                    this.getEffectTimeAfter(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.GTE
            );
        }
        if (this.getEffectTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    effectTimePath,
                    this.getEffectTimeBefore(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getEffectTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        effectTimePath,
                        null,
                        OffsetDateTime.class
                ));
            }
            fieldDefinition.and(beforeDefinition);
        }
    }

    protected FieldDefinition applyEffectTimes(String basePath) {
        FieldDefinition fieldDefinition = null;
        String effectTimePath = basePath + LookupDefinition.separator + CprBitemporalRecord.DB_FIELD_EFFECT_FROM;
        if (this.getEffectTimeAfter() != null) {
            FieldDefinition afterDefinition = new FieldDefinition(
                    effectTimePath,
                    this.getEffectTimeAfter(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.GTE
            );
            fieldDefinition = afterDefinition;
        }
        if (this.getEffectTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    effectTimePath,
                    this.getEffectTimeBefore(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getEffectTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        effectTimePath,
                        null,
                        OffsetDateTime.class
                ));
            }
            fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, beforeDefinition);
        }
        return fieldDefinition;
    }

    public static FieldDefinition and(FieldDefinition left, FieldDefinition right) {
        if (right == null) return left;
        if (left == null) return right;
        left.and(right);
        return left;
    }

    protected FieldDefinition fromPath(String basePath) {
        FieldDefinition fieldDefinition = null;
        fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, this.applyOriginTimes(basePath));
        fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, this.applyRegistrationTimes(basePath));
        fieldDefinition = PersonStatisticsQuery.and(fieldDefinition, this.applyEffectTimes(basePath));
        return fieldDefinition;
    }

}
