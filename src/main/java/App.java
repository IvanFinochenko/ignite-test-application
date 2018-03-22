public class App {
    String getGreeting() {
        return "Hello world. (v2)";
    }

    public static void main(String[] args) {
        System.out.println(new App().getGreeting());
    }
}