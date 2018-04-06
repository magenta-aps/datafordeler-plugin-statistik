package dk.magenta.datafordeler.statistik;


import org.springframework.web.bind.annotation.RequestMapping;

import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;

@RestController
@RequestMapping("/statistik/birth_data")
public class BirthDataService {

    @RequestMapping("/greeting")
    public String greeting() {
        return "All good in here...";
    }



}
