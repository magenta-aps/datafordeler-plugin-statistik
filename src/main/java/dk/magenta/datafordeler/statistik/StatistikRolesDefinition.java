package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.plugin.RolesDefinition;
import dk.magenta.datafordeler.core.role.ExecuteCommandRole;
import dk.magenta.datafordeler.core.role.ExecuteCommandRoleVersion;
import dk.magenta.datafordeler.core.role.ReadServiceRole;
import dk.magenta.datafordeler.core.role.SystemRole;

import java.util.Collections;
import java.util.List;

public class StatistikRolesDefinition extends RolesDefinition {

    public static ExecuteCommandRole EXECUTE_STATISTIK_ROLE = new ExecuteCommandRole(
            "statistik",
            null,
            new ExecuteCommandRoleVersion(
                    1.0f,
                    "Role that gives access to generating statistics"
            )
    );

    @Override
    public List<SystemRole> getRoles() {
        return Collections.singletonList(EXECUTE_STATISTIK_ROLE);
    }

    @Override
    public ReadServiceRole getDefaultReadRole() {
        return null;
    }
}
