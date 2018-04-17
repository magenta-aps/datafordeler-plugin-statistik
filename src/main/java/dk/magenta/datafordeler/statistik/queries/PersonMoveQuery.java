package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

public class PersonMoveQuery extends PersonQuery {

    private OffsetDateTime moveDateTimeBefore = null;

    public void setMoveDateTimeBefore(LocalDateTime moveDateTimeBefore) {
        this.moveDateTimeBefore = moveDateTimeBefore.atOffset(ZoneOffset.UTC);
    }

    public void setMoveDateTimeBefore(OffsetDateTime moveDateTimeBefore) {
        this.moveDateTimeBefore = moveDateTimeBefore;
    }


    private OffsetDateTime moveDateTimeAfter = null;

    public void setMoveDateTimeAfter(LocalDateTime moveDateTimeAfter) {
        this.moveDateTimeAfter = moveDateTimeAfter.atOffset(ZoneOffset.UTC);
    }

    public void setMoveDateTimeAfter(OffsetDateTime moveDateTimeAfter) {
        this.moveDateTimeAfter = moveDateTimeAfter;
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        if (this.moveDateTimeAfter != null || this.moveDateTimeBefore != null) {
            LookupDefinition.FieldDefinition fieldDefinition = lookupDefinition.put(PersonBaseData.DB_FIELD_MOVEMUNICIPALITY, null, Integer.class, LookupDefinition.Operator.NE);

            if (this.moveDateTimeAfter != null) {
                LookupDefinition.FieldDefinition effectFromDef = fieldDefinition.and(
                        LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                        this.moveDateTimeAfter,
                        OffsetDateTime.class,
                        LookupDefinition.Operator.GTE
                );
                effectFromDef.or(
                        LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                        null,
                        OffsetDateTime.class
                );
            }

            if (this.moveDateTimeBefore != null) {
                LookupDefinition.FieldDefinition effectToDef = fieldDefinition.and(
                        LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                        this.moveDateTimeBefore,
                        OffsetDateTime.class,
                        LookupDefinition.Operator.LTE
                );
                effectToDef.or(
                        LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                        null,
                        OffsetDateTime.class
                );
            }
        }

        return lookupDefinition;
    }

}
