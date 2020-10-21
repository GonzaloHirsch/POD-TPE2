package ar.edu.itba.pod.client;

import java.io.FileWriter;
import java.io.IOException;
import java.time.Instant;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

public class CustomLogger {
    private static final CustomLogger INSTANCE = new CustomLogger();
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter
            .ofPattern("dd/MM/yyyy HH:mm:ss")
            .withZone(ZoneId.systemDefault());

    private CustomLogger() {
    }

    public static CustomLogger GetInstance() {
        return INSTANCE;
    }

    /**
     * Given a filename and a message, it outputs a log with time and the message
     * @param filename Path to the file
     * @param message Message to be written
     */
    public static void Write(String filename, String message) {
        try {
            FileWriter myWriter = new FileWriter(filename);
            myWriter.write(FORMATTER.format(Instant.now()) + " - " + message + "\n");
            myWriter.close();
        } catch (IOException e) {
            System.out.println("An error occurred when writing log to " + filename);
        }
    }
}
