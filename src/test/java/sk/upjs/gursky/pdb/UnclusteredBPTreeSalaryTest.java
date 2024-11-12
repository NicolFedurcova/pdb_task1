package sk.upjs.gursky.pdb;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertTrue;

public class UnclusteredBPTreeSalaryTest {

    private UnclusteredBPTreeSalary bptree;

    @Before
    public void setUp() throws Exception {
        bptree = UnclusteredBPTreeSalary.createByBulkLoading();
    }

    @After
    public void tearDown() throws Exception {
        bptree.close();
        UnclusteredBPTreeSalary.INDEX_FILE.delete();
    }

    @Test
    public void test() throws Exception {
        long time = System.nanoTime();
        List<PersonEntry> result = bptree.unclusteredSalaryIntervalQuery(new SalaryKey(500), new SalaryKey(1000));
        time = System.nanoTime() - time;

        System.out.println("Interval uncluseteredSalary: " + time/1_000_000.0 +" ms");

        for (PersonEntry e : result){
            assertTrue((500<=e.salary) && (e.salary<=1000));
        }
    }
}
