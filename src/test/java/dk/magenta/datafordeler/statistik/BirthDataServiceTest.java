package dk.magenta.datafordeler.statistik;

import org.junit.Test;
import org.junit.runner.RunWith;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.equalTo;

@RunWith(SpringRunner.class)
@SpringBootTest
public class BirthDataServiceTest {

    private BirthDataService birthDataService;

    @Test
    public void greetingTest(){
        birthDataService = new BirthDataService();

        assertThat("All good in here...", is(equalTo(birthDataService.greeting())));
    }
}
