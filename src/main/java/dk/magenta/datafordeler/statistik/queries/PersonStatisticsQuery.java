package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.core.fapi.Query;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.statistik.services.StatisticsService;

import javax.servlet.http.HttpServletRequest;
import java.time.OffsetDateTime;

public class PersonStatisticsQuery extends PersonQuery {

    public PersonStatisticsQuery(HttpServletRequest request) {
        this.setRegistrationTimeBefore(Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_BEFORE)));
        this.setRegistrationTimeAfter(Query.parseDateTime(request.getParameter(StatisticsService.REGISTRATION_AFTER)));
        this.setEffectTimeBefore(Query.parseDateTime(request.getParameter(StatisticsService.BEFORE_DATE_PARAMETER)));
        this.setEffectTimeAfter(Query.parseDateTime(request.getParameter(StatisticsService.AFTER_DATE_PARAMETER)));
        String pnr = request.getParameter("pnr");
        if (pnr != null) {
            this.setPersonnummer(pnr);
        }
        this.setPageSize(1000000);
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


    protected void applyRegistrationTimes(FieldDefinition fieldDefinition) {
        if (this.getRegistrationTimeAfter() != null) {
            fieldDefinition.and(
                    LookupDefinition.registrationref + LookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                    this.getRegistrationTimeAfter(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.GTE
            );
        }
        if (this.getRegistrationTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    LookupDefinition.registrationref + LookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                    this.getRegistrationTimeBefore(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getRegistrationTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        LookupDefinition.registrationref + LookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                        null,
                        OffsetDateTime.class
                ));
            }
            fieldDefinition.and(beforeDefinition);
        }
    }


    protected void applyEffectTimes(FieldDefinition fieldDefinition) {
        if (this.getEffectTimeAfter() != null) {
            fieldDefinition.and(
                    LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                    this.getEffectTimeAfter(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.GTE
            );
        }
        if (this.getEffectTimeBefore() != null) {
            FieldDefinition beforeDefinition = new FieldDefinition(
                    LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                    this.getEffectTimeBefore(),
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            if (this.getEffectTimeAfter() == null) {
                beforeDefinition.or(new FieldDefinition(
                        LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                        null,
                        OffsetDateTime.class
                ));
            }
            fieldDefinition.and(beforeDefinition);
        }
    }

}
