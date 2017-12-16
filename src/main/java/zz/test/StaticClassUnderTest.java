package zz.test;


public class StaticClassUnderTest {

    static volatile ThreadLocal<String> value = new ThreadLocal();

    public static String go1(String s) {
        value.set(s);
        try {
            Thread.sleep(100);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
        return s+":"+value.get();
    }

}
