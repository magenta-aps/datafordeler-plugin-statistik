package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.configuration.ConfigurationManager;
import dk.magenta.datafordeler.core.plugin.AreaRestrictionDefinition;
import dk.magenta.datafordeler.core.plugin.Plugin;
import dk.magenta.datafordeler.core.plugin.RegisterManager;
import dk.magenta.datafordeler.core.plugin.RolesDefinition;
import org.springframework.stereotype.Component;

@Component
public class StatistikPlugin extends Plugin {

    StatistikRolesDefinition rolesDefinition = new StatistikRolesDefinition();

    @Override
    public String getName() {
        return "statistik";
    }

    @Override
    public RegisterManager getRegisterManager() {
        return null;
    }

    @Override
    public ConfigurationManager getConfigurationManager() {
        return null;
    }

    @Override
    public RolesDefinition getRolesDefinition() {
        return this.rolesDefinition;
    }

    @Override
    public AreaRestrictionDefinition getAreaRestrictionDefinition() {
        return null;
    }
}
