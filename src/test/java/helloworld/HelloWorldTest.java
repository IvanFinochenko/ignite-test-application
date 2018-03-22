package helloworld;

import org.junit.Test;

public class HelloWorldTest {

    @Test
    public void testJoin() {
        String testGreeting = "Hello, my first gradle project!";
        HelloWorld hw = new HelloWorld(testGreeting);

        hw.saySmth();
    }

}
