package org.example;

public class Consumer {
    public static void main(String[] args) {

        int consumerCount = 4;

        for (int i = 0; i < consumerCount; i++) {
            new Thread(Main::consume, "consumer-" + i).start();
        }
    }
}
