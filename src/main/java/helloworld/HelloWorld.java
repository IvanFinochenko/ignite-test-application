package helloworld;

public class HelloWorld {
    private String greeting;

    public HelloWorld(String greeting) {
        this.greeting = greeting;
    }

    public void saySmth() {
        System.out.println(greeting);
    }
}
