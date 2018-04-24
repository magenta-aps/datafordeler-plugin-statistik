package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonAddressData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonStatusData;
import dk.magenta.datafordeler.statistik.utils.Lookup;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;

public class PersonStatusQuery extends PersonQuery {

    private OffsetDateTime livingInGreenlandOn = null;

    public void setLivingInGreenlandOn(LocalDateTime livingInGreenlandOn) {
        this.livingInGreenlandOn = livingInGreenlandOn.atOffset(ZoneOffset.UTC);
    }

    public void setLivingInGreenlandOn(OffsetDateTime livingInGreenlandOn) {
        this.livingInGreenlandOn = livingInGreenlandOn;
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        if (this.livingInGreenlandOn != null) {
            ArrayList<LookupDefinition.FieldDefinition> fieldDefinitions = new ArrayList<>();
            fieldDefinitions.add(lookupDefinition.put(
                    PersonBaseData.DB_FIELD_ADDRESS + LookupDefinition.separator + PersonAddressData.DB_FIELD_MUNICIPALITY_CODE,
                    900,
                    Integer.class,
                    LookupDefinition.Operator.GTE
            ));
            fieldDefinitions.add(lookupDefinition.put(
                    PersonBaseData.DB_FIELD_STATUS + LookupDefinition.separator + PersonStatusData.DB_FIELD_STATUS,
                    90,
                    Integer.class,
                    LookupDefinition.Operator.NE
            ));

            for (LookupDefinition.FieldDefinition fieldDefinition : fieldDefinitions) {
                fieldDefinition.and(
                        LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                        this.livingInGreenlandOn,
                        OffsetDateTime.class,
                        LookupDefinition.Operator.LTE
                ).or(
                        LookupDefinition.effectref + lookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                        null,
                        OffsetDateTime.class
                );

                fieldDefinition.and(
                        LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_TO,
                        this.livingInGreenlandOn,
                        OffsetDateTime.class
                ).or(
                        LookupDefinition.effectref + lookupDefinition.separator + Effect.DB_FIELD_EFFECT_TO,
                        null,
                        OffsetDateTime.class
                );
            }
        }

        return lookupDefinition;
    }

}
