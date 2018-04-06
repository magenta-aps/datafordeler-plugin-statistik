package dk.magenta.datafordeler.statistik;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;

@RunWith(SpringRunner.class)
@SpringBootTest
public class DeathDataServiceTest {

    private DeathDataService deathDataService;

    @Test
    public void greetingTest(){

        deathDataService = new DeathDataService();

        assertThat("All good in here...", is(equalTo(deathDataService.greeting())));
    }

}
