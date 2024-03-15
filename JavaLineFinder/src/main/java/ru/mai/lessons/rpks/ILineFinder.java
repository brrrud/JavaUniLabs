package ru.mai.lessons.rpks;

import ru.mai.lessons.rpks.exception.LineCountShouldBePositiveException;

import java.io.FileNotFoundException;
import java.util.concurrent.ExecutionException;

public interface ILineFinder {
  public void find(String inputFilename, String outputFilename, String keyWord,
                   int lineCount) throws LineCountShouldBePositiveException, FileNotFoundException, InterruptedException, ExecutionException; // запускает поиск
}
