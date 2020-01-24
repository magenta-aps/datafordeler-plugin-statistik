package dk.magenta.datafordeler.statistik.utils;

import dk.magenta.datafordeler.statistik.services.StatisticsService;
import javax.servlet.http.HttpServletRequest;

public class CivilStatusFilter extends Filter {

    private String civilStatus;
    private String eventName;


    public CivilStatusFilter(HttpServletRequest request, boolean timeintervallimit) throws Exception {
        super(request, timeintervallimit);
        this.civilStatus = request.getParameter(StatisticsService.CIVIL_STATUS);
        this.eventName = request.getParameter(StatisticsService.EVENT_NAME);
    }


    public String getCivilStatus() {
        return civilStatus;
    }

    public String getEventName() {
        return eventName;
    }

    public void setCivilStatus(String civilStatus) {
        this.civilStatus = civilStatus;
    }

    @Override
    public String toString() {
        return "Filter{" +
                "effectAt=" + effectAt +
                ", after=" + after +
                ", before=" + before +
                ", registrationAfter=" + registrationAfter +
                ", registrationBefore=" + registrationBefore +
                ", registrationAt=" + registrationAt +
                ", originAfter=" + originAfter +
                ", originBefore=" + originBefore +
                ", onlyPnr=" + onlyPnr +
                ", civilStatus=" + civilStatus +
                '}';
    }
}
