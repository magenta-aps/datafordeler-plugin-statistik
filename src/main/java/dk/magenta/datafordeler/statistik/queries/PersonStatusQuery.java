package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.PersonEntity;
import dk.magenta.datafordeler.cpr.records.person.data.AddressDataRecord;
import dk.magenta.datafordeler.cpr.records.person.data.PersonStatusDataRecord;
import dk.magenta.datafordeler.statistik.utils.Filter;

import javax.servlet.http.HttpServletRequest;
import java.util.ArrayList;

public class PersonStatusQuery extends PersonStatisticsQuery {


    public PersonStatusQuery(HttpServletRequest request) {
        super(request);
    }

    public PersonStatusQuery(Filter filter) {
        super(filter);
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

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
            applyRegistrationTimes(fieldDefinition);
            applyEffectTimes(fieldDefinition);
            lookupDefinition.put(fieldDefinition);
        }

        return lookupDefinition;
    }

}
