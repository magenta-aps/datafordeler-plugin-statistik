package dk.magenta.datafordeler.statistik;

import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/statistik/movement_data")
public class MovementDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }

}
