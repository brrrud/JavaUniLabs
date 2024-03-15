package ru.mai.lessons.rpks.impl;


import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;
import ru.mai.lessons.rpks.ILineFinder;
import ru.mai.lessons.rpks.exception.LineCountShouldBePositiveException;

import java.io.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Slf4j
public class LineFinder implements ILineFinder {
    @Override
    public void find(String inputFilename, String outputFilename, String keyWord, int lineCount) throws LineCountShouldBePositiveException{
        inputFilename = "src/test/resources/" + inputFilename;
        int blockSize = 1024;
        if (lineCount < 0) {
            throw new LineCountShouldBePositiveException("lineCount should be positive");
        }
        try {
            File file = new File(inputFilename);
            long fileSize = file.length();
            System.out.println(fileSize);
            Set<Pair<Long, Long>> g = new HashSet<>();
            ExecutorService executor = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors());
            for (long position = 0; position < fileSize; position += blockSize) {
                long endPosition = Math.min(position + blockSize, fileSize);
                long finalPosition = position;
                String finalInputFilename = inputFilename;
                g.addAll(executor.submit(() -> threadTask(finalInputFilename, finalPosition, endPosition, keyWord, lineCount)).get());
            }
            System.out.println(g);
            executor.shutdown();
            if (!executor.awaitTermination(10, TimeUnit.MINUTES)) {
                System.out.println("Not 10 min");
            }
            try (RandomAccessFile bufferedWriter = new RandomAccessFile("src/test/resources" + outputFilename, "rw")) {
                try (RandomAccessFile raf = new RandomAccessFile(inputFilename, "r")) {
                    for (Pair<Long, Long> i : g) {
                        raf.seek(i.getLeft());
                        long offset = i.getRight() - i.getLeft();
                        byte[] buff = new byte[(int) offset];
                        raf.readFully(buff);
                        bufferedWriter.write(buff);
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        } catch (ExecutionException | InterruptedException e) {
            log.error("ex");
        }
    }
    private ArrayList<Pair<Long, Long>> threadTask(String inputFileName, long startPosition, long endPosition, String keyWord, int lineCount) throws FileNotFoundException {
        try (RandomAccessFile raf = new RandomAccessFile(inputFileName, "r")) {
            raf.seek(endPosition);
            long endRealpos = endPosition;
            while (raf.getFilePointer() < raf.length() && !Character.isWhitespace(raf.readByte())) {
                endRealpos++;
            }
            raf.seek(startPosition);
            byte[] buffer = new byte[(int) (endRealpos - startPosition)];
            raf.readFully(buffer);
            String segment = new String(buffer, StandardCharsets.UTF_8);
            ByteBuffer byteBuffer = ByteBuffer.wrap(buffer);
            byte[] keyWordBytes = keyWord.getBytes(StandardCharsets.UTF_8);
            ArrayList<Pair<Long, Long>> indexes = new ArrayList<>();
            for (int i = 0; i < buffer.length - keyWordBytes.length; i++) {
                byte[] currentBytes = new byte[keyWordBytes.length];
                byteBuffer.get(currentBytes);
                if (Arrays.equals(currentBytes, keyWordBytes)) {
                    indexes.add(new ImmutablePair<>(getLeftBorder(raf, i+startPosition, lineCount),
                            getRightBorder(raf, i + startPosition, lineCount)));
                    //indexes.add((long) i + startPosition);
                }
                byteBuffer.position(byteBuffer.position() - keyWordBytes.length + 1);
            }
            return indexes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    private Long getRightBorder(RandomAccessFile raf, long positionKeyword, int lineCount) throws IOException {
        int numberLines = 0;
        int rightOffset = 0;
        raf.seek(positionKeyword);
        long possibleOffset = raf.length() - positionKeyword;
        while (rightOffset++ < possibleOffset && numberLines <= lineCount) {
            if (raf.readByte() == '\n') {
                numberLines++;
            }
        }
        return raf.getFilePointer();
    }
    private Long getLeftBorder(RandomAccessFile raf, long positionKeyword, int lineCount) throws IOException {
        long leftOffset = 0;
        int numberLines = 0;
        raf.seek(positionKeyword);
        long findPosition = raf.getFilePointer();
        while (leftOffset < findPosition && numberLines <= lineCount) {
            if (raf.readByte() == '\n') {
                numberLines++;
            }

            leftOffset++;
            raf.seek(findPosition - leftOffset);
        }

        if (leftOffset < findPosition)
        {
            leftOffset -= 2;
            raf.seek(findPosition - leftOffset);
        }
        return raf.getFilePointer();
    }


}