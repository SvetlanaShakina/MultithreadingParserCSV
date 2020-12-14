package fileparser;

import java.io.*;
import java.util.*;

public class MultithreadingParserCSV {
    private static final Map<String, Set<String>> namesAndContentsSourceFiles = new HashMap<>();

    public static void main(String[] args) {
        wait(createReaders(args));
        createWriters();
    }

    private static List<Thread> createReaders(String[] args) {
        List<Thread> threadReaderList = new ArrayList<>();
        for (String arg : args) {
            Thread thread = new Thread(new ReaderFiles(), arg);
            thread.start();
            threadReaderList.add(thread);
        }
        return threadReaderList;
    }

    private static void wait(List<Thread> threads) {
        threads.forEach(thread -> {
            try {
                thread.join();
            } catch (InterruptedException exception) {
                exception.printStackTrace();
            }
        });
    }

    private static void createWriters() {
        namesAndContentsSourceFiles.entrySet().stream().map(str -> new Thread(new WriterFiles(str.getValue()), str.getKey()))
                .forEach(Thread::start);
    }


    private static class ReaderFiles implements Runnable {
        public void run() {
            try (BufferedReader reader = new BufferedReader(new FileReader(Thread.currentThread().getName()))) {
                String[] newFilesNames = reader.readLine().split(";");
                Arrays.stream(newFilesNames).forEach(str -> namesAndContentsSourceFiles.putIfAbsent(str, new LinkedHashSet<>()));
                String line;
                while ((line = reader.readLine()) != null) {
                    String[] text = line.split(";");
                    for (int i = 0; i < text.length; i++) {
                        synchronized (namesAndContentsSourceFiles) {
                            namesAndContentsSourceFiles.get(newFilesNames[i]).add(text[i] + ";");
                        }
                    }
                }
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }

    private static class WriterFiles implements Runnable {
        private final Set<String> text;

        public WriterFiles(Set<String> text) {
            this.text = text;
        }

        public void run() {
            try (BufferedWriter bufferedWriter = new BufferedWriter(new OutputStreamWriter(new FileOutputStream(Thread.currentThread().getName() + ".csv"), "cp1251"))) {
                for (String str : text) {
                    bufferedWriter.write(str);
                }
            } catch (IOException exception) {
                exception.printStackTrace();
            }
        }
    }

}
