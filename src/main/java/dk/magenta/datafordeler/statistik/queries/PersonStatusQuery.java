package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.PersonRecordQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonAddressData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonStatusData;
import dk.magenta.datafordeler.cpr.records.CprBitemporalRecord;
import dk.magenta.datafordeler.cpr.records.person.data.AddressDataRecord;
import dk.magenta.datafordeler.cpr.records.person.data.PersonStatusDataRecord;
import dk.magenta.datafordeler.statistik.services.StatisticsService;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.util.ArrayList;

public class PersonStatusQuery extends PersonRecordQuery {

    private OffsetDateTime livingInGreenlandOn = null;

    public void setLivingInGreenlandOn(LocalDateTime livingInGreenlandOn) {
        this.livingInGreenlandOn = livingInGreenlandOn.atZone(StatisticsService.cprDataOffset).toOffsetDateTime();
    }

    public void setLivingInGreenlandOn(OffsetDateTime livingInGreenlandOn) {
        this.livingInGreenlandOn = livingInGreenlandOn;
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        if (this.livingInGreenlandOn != null) {
            ArrayList<FieldDefinition> fieldDefinitions = new ArrayList<>();
            fieldDefinitions.add(new FieldDefinition(
                    PersonEntity.DB_FIELD_ADDRESS + LookupDefinition.separator + AddressDataRecord.DB_FIELD_MUNICIPALITY_CODE,
                    900,
                    Integer.class,
                    LookupDefinition.Operator.GTE
            ));
            fieldDefinitions.add(new FieldDefinition(
                    PersonEntity.DB_FIELD_STATUS + LookupDefinition.separator + PersonStatusDataRecord.DB_FIELD_STATUS,
                    90,
                    Integer.class,
                    LookupDefinition.Operator.NE
            ));

            for (FieldDefinition fieldDefinition : fieldDefinitions) {
                String path = PersonStatisticsQuery.cutPath(fieldDefinition.path);
                fieldDefinition.and(
                        path + lookupDefinition.separator + CprBitemporalRecord.DB_FIELD_EFFECT_FROM,
                        this.livingInGreenlandOn,
                        OffsetDateTime.class,
                        LookupDefinition.Operator.LTE
                ).or(
                        path + lookupDefinition.separator + CprBitemporalRecord.DB_FIELD_EFFECT_FROM,
                        null,
                        OffsetDateTime.class
                );

                fieldDefinition.and(
                        path + lookupDefinition.separator + CprBitemporalRecord.DB_FIELD_EFFECT_TO,
                        this.livingInGreenlandOn,
                        OffsetDateTime.class,
                        LookupDefinition.Operator.GTE
                ).or(
                        path + lookupDefinition.separator + CprBitemporalRecord.DB_FIELD_EFFECT_TO,
                        null,
                        OffsetDateTime.class
                );

                lookupDefinition.put(fieldDefinition);
            }
        }

        return lookupDefinition;
    }

}
