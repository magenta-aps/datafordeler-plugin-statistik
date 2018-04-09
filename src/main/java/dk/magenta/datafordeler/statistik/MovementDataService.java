package dk.magenta.datafordeler.statistik;
/*ract the following for a person:
    pnr
    birth year
    effective pnr
    status code
    birth municipality code (data missing, import handled in another ticket)
    mother's pnr
    father's pnr
    spouse's pnr
    movement date
    prod date (to be investigated)
    origin municipality code
    origin locality name
    origin road code
    origin house number
    origin floor
    origin door/apartment no.
    origin bnr
    destination municipality code
    destination locality name
    destination road code
    destination house number
    destination floor
    destination door/apartment no.
    destination bnr

Input parameters:
    movement date*/
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

/*Created by Efrin 06-04-2018*/


@RestController
@RequestMapping("/statistik/movement_data")
public class MovementDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }

}
