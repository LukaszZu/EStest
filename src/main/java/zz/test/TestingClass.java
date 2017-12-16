package zz.test;

import java.util.stream.IntStream;

import static java.util.stream.Collectors.toList;

public class TestingClass {
    public static void main(String[] args) {
        IntStream.range(1,100).boxed().parallel()
                .map(String::valueOf)
                .map(StaticClassUnderTest::go1)
                .collect(toList())
        .forEach(System.out::println);
    }
}
