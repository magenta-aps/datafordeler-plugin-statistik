package dk.magenta.datafordeler.statistik.utils;

import dk.magenta.datafordeler.statistik.services.StatisticsService;
import javax.servlet.http.HttpServletRequest;

public class CivilStatusFilter extends Filter {

    public  String civilStatus;


    public CivilStatusFilter(HttpServletRequest request) {
        super(request);
        this.civilStatus = request.getParameter(StatisticsService.CIVIL_STATUS);
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
