package ru.mai.lessons.rpks.impl;


import lombok.extern.slf4j.Slf4j;
import org.javatuples.Triplet;
import org.json.JSONArray;
import org.json.JSONException;
import org.json.JSONObject;
import ru.mai.lessons.rpks.IBracketsDetector;
import ru.mai.lessons.rpks.result.ErrorLocationPoint;

import java.util.*;

@Slf4j
public class BracketsDetector implements IBracketsDetector {
    private Map<Character, Character> getBracketsFromConfig(String configString) {
        try {
            Map<Character, Character> bracketsMap = new HashMap<>();
            JSONArray jsonArray = new JSONObject(configString).getJSONArray("bracket");
            for (Object i : jsonArray) {
                bracketsMap.put((((JSONObject) i).get("right")).toString().charAt(0), (((JSONObject) i).get("left")).toString().charAt(0));
            }
            return bracketsMap;
        } catch (JSONException e) {
            log.error("JSON parse exception");
            throw e;
        }
    }

    @Override
    public List<ErrorLocationPoint> check(String config, List<String> content) {
        List<ErrorLocationPoint> errorLocationPoints = new ArrayList<>();
        Deque<Triplet<Character, Integer, Integer>> stackContent = new ArrayDeque<>();
        Map<Character, Character> bracketsConfig = getBracketsFromConfig(config);
        Iterator<String> lineIterator = content.iterator();
        int contentLineNum = 1;
        while (lineIterator.hasNext()) {
            String line = lineIterator.next();
            int symbolNum = 0;
            int countIdenticalBrackets = 0;
            int errorIndexIdenticalBrackets = 0;
            for (int i = 0; i < line.length(); i++) {
                char symbol = line.charAt(i);
                symbolNum++;

                if (bracketsConfig.containsValue(symbol)) {
                    if (symbol == '|') {
                        countIdenticalBrackets++;
                        errorIndexIdenticalBrackets = symbolNum;
                    } else {
                        stackContent.push(new Triplet<>(symbol, contentLineNum, symbolNum));
                    }

                } else if (bracketsConfig.containsKey(symbol)) {
                    if (stackContent.isEmpty() || stackContent.peek().getValue0() != bracketsConfig.get(symbol)) {
                        errorLocationPoints.add(new ErrorLocationPoint(contentLineNum, symbolNum));
                    } else {
                        stackContent.pop();
                    }

                }

            }

            if (countIdenticalBrackets % 2 != 0) {
                errorLocationPoints.add(new ErrorLocationPoint(contentLineNum, errorIndexIdenticalBrackets));
            }
            while (!stackContent.isEmpty()) {
                Triplet<Character, Integer, Integer> el = stackContent.pop();
                errorLocationPoints.add(new ErrorLocationPoint(el.getValue1(), el.getValue2()));
            }

            contentLineNum++;
        }

        return errorLocationPoints;
    }
}
