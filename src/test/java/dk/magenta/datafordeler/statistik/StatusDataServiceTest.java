package dk.magenta.datafordeler.statistik;

import dk.magenta.datafordeler.statistik.services.StatusDataService;
import org.hamcrest.MatcherAssert;
import org.hamcrest.Matchers;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@RunWith(SpringRunner.class)
@SpringBootTest
public class StatusDataServiceTest {

    private StatusDataService statusDataService;

    @Test
    public void greetingTest(){
        statusDataService = new StatusDataService();
        MatcherAssert.assertThat("All good in here...", Matchers.is(equalTo(statusDataService.greeting())));
    }
}
