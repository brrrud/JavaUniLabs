package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.IFileReader;
import ru.mai.lessons.rpks.exception.FilenameShouldNotBeEmptyException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;

@Slf4j
public class FileReader implements IFileReader {
    @Override
    public List<String> loadContent(String filePath) throws FilenameShouldNotBeEmptyException {
        if (filePath == null || filePath.isEmpty()) {
            throw new FilenameShouldNotBeEmptyException("Invalid file in loadContent function " + filePath);
        }
        try {
            return Files.readAllLines(Paths.get("src/test/resources/" + filePath));
        } catch (IOException e) {
            log.error("Cannot read file", e);
            throw new FilenameShouldNotBeEmptyException("bad path");
        }
    }
}