package dk.magenta.datafordeler.statistik;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/statistik/status_data")
public class StatusDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }

}
