package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.Effect;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.core.database.Registration;
import dk.magenta.datafordeler.cpr.data.person.PersonQuery;
import dk.magenta.datafordeler.cpr.data.person.data.PersonAddressData;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;

import java.time.LocalDateTime;
import java.time.OffsetDateTime;
import java.time.ZoneOffset;

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
            LookupDefinition.FieldDefinition fieldDefinition = lookupDefinition.put(
                    PersonBaseData.DB_FIELD_ADDRESS + LookupDefinition.separator + PersonAddressData.DB_FIELD_MUNICIPALITY_CODE,
                    900,
                    Integer.class,
                    LookupDefinition.Operator.GTE
            );

            LookupDefinition.FieldDefinition effectFromDef = fieldDefinition.and(
                    LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                    this.livingInGreenlandOn,
                    OffsetDateTime.class,
                    LookupDefinition.Operator.LTE
            );
            effectFromDef.or(
                    LookupDefinition.effectref + lookupDefinition.separator + Effect.DB_FIELD_EFFECT_FROM,
                    null,
                    OffsetDateTime.class
            );

            LookupDefinition.FieldDefinition effectToDef = fieldDefinition.and(
                    LookupDefinition.effectref + LookupDefinition.separator + Effect.DB_FIELD_EFFECT_TO,
                    this.livingInGreenlandOn,
                    OffsetDateTime.class
            );
            effectToDef.or(
                    LookupDefinition.effectref + lookupDefinition.separator + Effect.DB_FIELD_EFFECT_TO,
                    null,
                    OffsetDateTime.class
            );

        }
        return lookupDefinition;
    }

}
