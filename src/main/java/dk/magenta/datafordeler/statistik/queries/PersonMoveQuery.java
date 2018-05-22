package dk.magenta.datafordeler.statistik.queries;

import dk.magenta.datafordeler.core.database.FieldDefinition;
import dk.magenta.datafordeler.core.database.LookupDefinition;
import dk.magenta.datafordeler.cpr.data.person.data.PersonBaseData;

import javax.servlet.http.HttpServletRequest;
import java.util.HashSet;

public class PersonMoveQuery extends PersonStatisticsQuery {

    public PersonMoveQuery(HttpServletRequest request) {
        super(request);
    }

    @Override
    public LookupDefinition getLookupDefinition() {
        LookupDefinition lookupDefinition = super.getLookupDefinition();
        lookupDefinition.setMatchNulls(true);

        HashSet<FieldDefinition> fieldDefinitions = new HashSet<>();
        fieldDefinitions.add(lookupDefinition.put(PersonBaseData.DB_FIELD_ADDRESS, null, Integer.class, LookupDefinition.Operator.NE));
        //fieldDefinitions.add(lookupDefinition.put(PersonBaseData.DB_FIELD_FOREIGN_ADDRESS, null, Integer.class, LookupDefinition.Operator.NE));
        fieldDefinitions.add(lookupDefinition.put(PersonBaseData.DB_FIELD_MIGRATION, null, Integer.class, LookupDefinition.Operator.NE));
        lookupDefinition.orDefinitions();

        for (FieldDefinition fieldDefinition : fieldDefinitions) {
            this.applyRegistrationTimes(fieldDefinition);
            this.applyEffectTimes(fieldDefinition);
        }

        return lookupDefinition;
    }

}
