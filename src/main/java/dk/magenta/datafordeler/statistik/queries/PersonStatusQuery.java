package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.BaseLookupDefinition;
import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.person.data.AddressDataRecord;
import dk.magenta.datafordeler.cpr.records.person.data.PersonStatusDataRecord;
import dk.magenta.datafordeler.statistik.utils.Filter;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;
import java.util.Arrays;

public class PersonStatusQuery extends PersonStatisticsQuery {


    public PersonStatusQuery(HttpServletRequest request) {
        super(request);
    }

    public PersonStatusQuery(Filter filter) {
        super(filter);
    }

    @Override
    public BaseLookupDefinition getLookupDefinition() {
        BaseLookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        ArrayList<FieldDefinition> fieldDefinitions = new ArrayList<>();
        FieldDefinition addressDefinition = new FieldDefinition(
                PersonEntity.DB_FIELD_ADDRESS + LookupDefinition.separator + AddressDataRecord.DB_FIELD_MUNICIPALITY_CODE,
                900,
                Integer.class,
                LookupDefinition.Operator.GTE
        );
        addressDefinition.and(PersonEntity.DB_FIELD_ADDRESS + LookupDefinition.separator + AddressDataRecord.DB_FIELD_UNDONE,
                false,
                Boolean.class,
                LookupDefinition.Operator.EQ);

        fieldDefinitions.add(addressDefinition);

/*
        FieldDefinition statusDefinition = new FieldDefinition(
                PersonEntity.DB_FIELD_STATUS + LookupDefinition.separator + PersonStatusDataRecord.DB_FIELD_STATUS,
                Arrays.asList(50, 60, 80, 90),
                Integer.class,
                LookupDefinition.Operator.NE
        );
        fieldDefinitions.add(statusDefinition);
*/
        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            applyRegistrationTimes(fieldDefinition);
            applyEffectTimes(fieldDefinition);
            lookupDefinition.put(fieldDefinition);
        }

        return lookupDefinition;
    }

}
