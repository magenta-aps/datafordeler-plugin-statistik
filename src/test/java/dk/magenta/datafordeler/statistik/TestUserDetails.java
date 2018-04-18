package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.core.arearestriction.AreaRestriction;
import dk.magenta.datafordeler.core.role.SystemRole;
import dk.magenta.datafordeler.core.user.DafoUserDetails;
import dk.magenta.datafordeler.core.user.UserProfile;

import java.util.*;

public class TestUserDetails extends DafoUserDetails {

    private HashMap<String, UserProfile> userProfiles = new HashMap<>();
    private HashMap<String, Set<UserProfile>> systemRoles = new HashMap<>();
    private static final String profileName = "TestProfile";

    public void addUserProfile(UserProfile userprofile) {
        this.userProfiles.put(userprofile.getName(), userprofile);
        this.commitUserProfiles();
    }

    private void commitUserProfiles() {
        for (UserProfile userprofile : this.userProfiles.values()) {
            for (String systemRole : userprofile.getSystemRoles()) {
                if (systemRoles.containsKey(systemRole)) {
                    systemRoles.get(systemRole).add(userprofile);
                } else {
                    HashSet<UserProfile> set = new HashSet<>();
                    set.add(userprofile);
                    systemRoles.put(systemRole, set);
                }
            }
        }
    }

    @Override
    public String getNameQualifier() {
        return "testing";
    }

    @Override
    public String getIdentity() {
        return "tester";
    }

    @Override
    public String getOnBehalfOf() {
        return null;
    }

    @Override
    public boolean isAnonymous() {
        return false;
    }

    @Override
    public boolean hasUserProfile(String userProfileName) {
        return userProfiles.containsKey(userProfileName);
    }

    @Override
    public Collection<String> getUserProfiles() {
        return userProfiles.keySet();
    }

    @Override
    public Collection<String> getSystemRoles() {
        return systemRoles.keySet();
    }

    @Override
    public boolean hasSystemRole(String role) {
        return systemRoles.containsKey(role);
    }

    @Override
    public Collection<UserProfile> getUserProfilesForRole(String role) {
        return systemRoles.getOrDefault(role, Collections.EMPTY_SET);
    }

    public void giveAccess(SystemRole... rolesDefinitions) {
        ArrayList<String> roleNames = new ArrayList<>();
        for (SystemRole role : rolesDefinitions) {
            roleNames.add(role.getRoleName());
        }
        UserProfile testUserProfile = this.userProfiles.get(profileName);
        if (testUserProfile == null) {
            testUserProfile = new UserProfile(profileName);
            this.addUserProfile(testUserProfile);
        }
        testUserProfile.addSystemRoles(roleNames);
        this.commitUserProfiles();
    }

    public void giveAccess(AreaRestriction... areaRestrictions) {
        UserProfile testUserProfile = this.userProfiles.get(profileName);
        if (testUserProfile == null) {
            testUserProfile = new UserProfile(profileName);
            this.addUserProfile(testUserProfile);
        }
        testUserProfile.addAreaRestrictions(Arrays.asList(areaRestrictions));
    }
}
