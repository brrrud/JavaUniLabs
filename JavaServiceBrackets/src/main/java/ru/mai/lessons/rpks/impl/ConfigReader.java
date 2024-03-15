package ru.mai.lessons.rpks.impl;

import lombok.extern.slf4j.Slf4j;
import ru.mai.lessons.rpks.IConfigReader;
import ru.mai.lessons.rpks.exception.FilenameShouldNotBeEmptyException;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
@Slf4j
public class ConfigReader implements IConfigReader {

    @Override
    public String loadConfig(String configPath) throws FilenameShouldNotBeEmptyException {
        if (configPath == null || configPath.isEmpty()) {
            log.error("FilenameShouldNotBeEmptyException");
            throw new FilenameShouldNotBeEmptyException("File path " + configPath + " is empty or null");
        }
        try {
            return readFileToString(configPath);
        } catch (IOException e) {
            log.error("Problem with reading file", e);
            throw new FilenameShouldNotBeEmptyException("Problem with reading file");
        }
    }

    public static String readFileToString(String filePath) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get("src/test/resources/" + filePath));
        StringBuilder content = new StringBuilder();

        for (String line : lines) {
            content.append(line);
            content.append(System.lineSeparator());
        }
        return content.toString();
    }
}