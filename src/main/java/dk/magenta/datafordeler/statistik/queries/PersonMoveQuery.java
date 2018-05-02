package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.statistik.services.StatisticsService;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.HashSet;

public class PersonMoveQuery extends PersonQuery {

    private OffsetDateTime moveDateTimeBefore = null;

    public void setMoveDateTimeBefore(LocalDateTime moveDateTimeBefore) {
        this.moveDateTimeBefore = moveDateTimeBefore.atZone(StatisticsService.cprDataOffset).toOffsetDateTime();
    }

    public void setMoveDateTimeBefore(OffsetDateTime moveDateTimeBefore) {
        this.moveDateTimeBefore = moveDateTimeBefore;
    }


    private OffsetDateTime moveDateTimeAfter = null;

    public void setMoveDateTimeAfter(LocalDateTime moveDateTimeAfter) {
        this.moveDateTimeAfter = moveDateTimeAfter.atZone(StatisticsService.cprDataOffset).toOffsetDateTime();
    }

    public void setMoveDateTimeAfter(OffsetDateTime moveDateTimeAfter) {
        this.moveDateTimeAfter = moveDateTimeAfter;
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        if (this.moveDateTimeAfter != null || this.moveDateTimeBefore != null) {
            HashSet<FieldDefinition> fieldDefinitions = new HashSet<>();
            fieldDefinitions.add(lookupDefinition.put(PersonBaseData.DB_FIELD_ADDRESS, null, Integer.class, LookupDefinition.Operator.NE));
            fieldDefinitions.add(lookupDefinition.put(PersonBaseData.DB_FIELD_FOREIGN_ADDRESS, null, Integer.class, LookupDefinition.Operator.NE));
            lookupDefinition.orDefinitions();

            for (FieldDefinition fieldDefinition : fieldDefinitions) {
                if (this.moveDateTimeAfter != null) {
                    FieldDefinition moveTimeDef = fieldDefinition.and(
                            LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                            this.moveDateTimeAfter,
                            OffsetDateTime.class,
                            LookupDefinition.Operator.GTE
                    );
                    moveTimeDef.or(
                            LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                            null,
                            OffsetDateTime.class
                    );
                }

                if (this.moveDateTimeBefore != null) {
                    FieldDefinition moveTimeDef = fieldDefinition.and(
                            LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                            this.moveDateTimeBefore,
                            OffsetDateTime.class,
                            LookupDefinition.Operator.LTE
                    );
                    moveTimeDef.or(
                            LookupDefinition.registrationref + lookupDefinition.separator + Registration.DB_FIELD_REGISTRATION_FROM,
                            null,
                            OffsetDateTime.class
                    );
                }
            }

        }

        return lookupDefinition;
    }

    /*@Override
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
    }*/


    /*@Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        if (this.moveDateTimeAfter != null || this.moveDateTimeBefore != null) {
            LookupDefinition.FieldDefinition fieldDefinition = lookupDefinition.put(PersonBaseData.DB_FIELD_MOVEMUNICIPALITY, null, Integer.class, LookupDefinition.Operator.NE);

            if (this.moveDateTimeAfter != null) {
                LookupDefinition.FieldDefinition effectFromDef = fieldDefinition.and(
                        PersonBaseData.DB_FIELD_MOVEMUNICIPALITY + lookupDefinition.separator + PersonMoveMunicipalityData.DB_FIELD_IN_DATETIME,
                        this.moveDateTimeAfter.toLocalDateTime(),
                        LocalDateTime.class,
                        LookupDefinition.Operator.GTE
                );
            }

            if (this.moveDateTimeBefore != null) {
                LookupDefinition.FieldDefinition effectToDef = fieldDefinition.and(
                        PersonBaseData.DB_FIELD_MOVEMUNICIPALITY + lookupDefinition.separator + PersonMoveMunicipalityData.DB_FIELD_IN_DATETIME,
                        this.moveDateTimeBefore.toLocalDateTime(),
                        LocalDateTime.class,
                        LookupDefinition.Operator.LTE
                );
            }
        }

        return lookupDefinition;
    }*/

}
