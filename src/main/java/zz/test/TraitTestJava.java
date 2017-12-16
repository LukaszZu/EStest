package zz.test;

import static zz.test.TraitTest.*;

/**
 * Created by zulk on 31.05.17.
 */
public class TraitTestJava {

    public void testTrait() {
        String a = TraitTest.apply("a").doSomething();
        System.out.println(a);

    }

    public static void main(String[] args) {
        TraitTestJava traitTestJava = new TraitTestJava();
        traitTestJava.testTrait();
    }
}
