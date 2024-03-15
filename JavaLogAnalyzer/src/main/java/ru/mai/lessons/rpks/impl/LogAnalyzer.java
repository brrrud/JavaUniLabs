package ru.mai.lessons.rpks.impl;

import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.ToString;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;
import ru.mai.lessons.rpks.ILogAnalyzer;
import ru.mai.lessons.rpks.exception.WrongFilenameException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.net.URISyntaxException;
import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.time.temporal.ChronoUnit;
import java.util.*;
import java.util.concurrent.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

@Slf4j
public class LogAnalyzer implements ILogAnalyzer {
    private static final int BLOCK_SIZE = 2097152;

    @Override
    public List<Integer> analyze(String filename, String deviation) throws WrongFilenameException {
        if (filename.isEmpty()) {
            throw new WrongFilenameException("Filename is empty or not found in analyze method");
        }

        try {
            Map<Integer, Long> completedLogsMap = readFilesChunks(filename);
            int medianTime = calculateDuration(completedLogsMap);
            List<Integer> invalidId = Collections.synchronizedList(new ArrayList<>());

            ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            completedLogsMap.forEach((key, value) ->
                    executorService.submit(() -> {
                        if (value - medianTime > getTimeInSecs(deviation)) {
                            invalidId.add(key);
                        }
                    })
            );

            executorService.shutdown();
            try {
                if (!executorService.awaitTermination(1000, TimeUnit.MILLISECONDS)) {
                    executorService.shutdownNow();
                }
            } catch (InterruptedException e) {
                executorService.shutdownNow();
                Thread.currentThread().interrupt();
            }

            invalidId.sort(Comparator.naturalOrder());
            return invalidId;
        } catch (ExecutionException | InterruptedException | IOException | URISyntaxException e) {
            log.error(e.getMessage());
            throw new WrongFilenameException("InterruptedException");
        }
    }

    private Map<Integer, Long> readFilesChunks(String inputFilename) throws IOException, URISyntaxException, ExecutionException, InterruptedException {
        ExecutorService executorService = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());

        try (RandomAccessFile fileInputStream = new RandomAccessFile(getFilePath(inputFilename), "r")) {
            List<Future<List<LogEntry>>> futures = new ArrayList<>();

            for (int bytesRead = 0; bytesRead < fileInputStream.length(); bytesRead += BLOCK_SIZE) {
                int finalBytesToRead = bytesRead;
                futures.add(executorService.submit(() -> threadReadTask(fileInputStream, finalBytesToRead)));
            }

            executorService.shutdown();
            if (!executorService.awaitTermination(1, TimeUnit.MINUTES)) {
                log.error("executorService shutdownNow()");
                executorService.shutdownNow();
            }


            Map<Integer, Long> completedRequests = new HashMap<>();
            Map<Integer, Pair<Boolean, LocalDateTime>> notCompletedRequests = new HashMap<>();

            for (Future<List<LogEntry>> item : futures) {
                if (item.isDone()) {
                    List<LogEntry> logEntries = item.get();
                    for (LogEntry entry : logEntries) {
                        int idLogs = entry.getId();
                        boolean statusLogs = entry.getStatus();
                        LocalDateTime timeRequest = entry.getTimestamp();
                        processLogEntry(completedRequests, notCompletedRequests, idLogs, statusLogs, timeRequest);
                    }
                }
            }

            return completedRequests;
        }
    }

    private void processLogEntry(Map<Integer, Long> completedRequests, Map<Integer, Pair<Boolean, LocalDateTime>> notCompletedRequests, int idLogs, boolean statusLogs, LocalDateTime timeRequest) {
        if (!statusLogs) {
            notCompletedRequests.put(idLogs, Pair.of(statusLogs, timeRequest));
        } else {
            Pair<Boolean, LocalDateTime> pair = notCompletedRequests.get(idLogs);
            if (pair != null) {
                completedRequests.put(idLogs, Math.abs(ChronoUnit.SECONDS.between(timeRequest, pair.getRight())));
                notCompletedRequests.remove(idLogs);
            } else {
                notCompletedRequests.put(idLogs, Pair.of(statusLogs, timeRequest));
            }
        }
    }


    private Pair<String, Integer> readToken(RandomAccessFile inputFile) throws IOException {
        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        byte byteRead;

        while ((byteRead = inputFile.readByte()) != '\n' && inputFile.getFilePointer() < inputFile.length()) {
            byteArrayOutputStream.write(byteRead);
        }
        return Pair.of(byteArrayOutputStream.toString(StandardCharsets.UTF_8), byteArrayOutputStream.size());
    }

    private List<LogEntry> threadReadTask(RandomAccessFile inputFile, int position) {
        try {
            inputFile.seek(position);
            int bytesRead = 0;

            List<LogEntry> logEntries = new ArrayList<>();

            Pair<String, Integer> token;
            while (bytesRead < BLOCK_SIZE && inputFile.getFilePointer() < inputFile.length()) {
                token = readToken(inputFile);

                LogEntry entry = parseLogEntry(token.getLeft());

                if (entry != null) {
                    logEntries.add(entry);
                }
                bytesRead += token.getRight();
            }
            return logEntries;
        } catch (IOException e) {
            log.error(e.getMessage());
            throw new RuntimeException(e);
        }
    }

    private static LogEntry parseLogEntry(String line) {
        DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
        String[] parts = line.split(" – ");

        if (parts.length < 3) return null;

        try {
            LocalDateTime timestamp = LocalDateTime.parse(parts[0], formatter);
            String message = parts[2];
            boolean status = message.contains("RESULT");
            int id = getIdLogs(message);
            return new LogEntry(timestamp, message, status, id);
        } catch (DateTimeParseException e) {
            log.error("Error parsing timestamp: " + e.getMessage());
        } catch (Exception e) {
            log.error(e.getMessage());
        }
        return null;
    }


    private static int getIdLogs(String message) {
        String tmp = StringUtils.stripEnd(message, "\r\n");
        return Integer.parseInt(tmp.substring(tmp.indexOf("=") + 2));
    }


    private static int calculateDuration(Map<Integer, Long> compiledLogs) {
        if (compiledLogs.isEmpty()) {
            return 0;
        }
        List<Integer> counting = new ArrayList<>();
        for (var item : compiledLogs.entrySet()) {
            addCounting(counting, item.getValue().intValue());
        }
        long tempCount = 0;
        int index = 0;
        while (tempCount < counting.stream().mapToInt(Integer::intValue).sum() / 2) {
            tempCount += counting.get(index++);
        }
        return index - 1;
    }

    private static void addCounting(List<Integer> listCounting, int timeExecution) {
        if (timeExecution >= listCounting.size()) {
            Integer[] temp = new Integer[timeExecution - listCounting.size() + 1];
            Arrays.fill(temp, 0);
            listCounting.addAll(List.of(temp));
        }
        listCounting.set(timeExecution, listCounting.get(timeExecution) + 1);
    }

    @AllArgsConstructor
    @Getter
    @ToString
    public static class LogEntry {
        private LocalDateTime timestamp;
        private String message;
        private Boolean status;
        private int id;
    }

    private String getFilePath(String filename) {
        return Objects.requireNonNull(getClass().getClassLoader().getResource(".")).getPath() + filename;
    }

    private int getTimeInSecs(String timeInString) {
        Matcher matcher = Pattern.compile("(\\d+)\\s*(sec|min|hour)s?").matcher(timeInString);
        if (matcher.find()) {
            int value = Integer.parseInt(matcher.group(1));
            String unit = matcher.group(2);
            return switch (unit) {
                case "sec" -> value;
                case "min" -> value * 60;
                case "hour" -> value * 3600;
                default -> 0;
            };
        }
        return 0;
    }
}