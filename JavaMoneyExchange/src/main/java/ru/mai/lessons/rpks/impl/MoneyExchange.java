package ru.mai.lessons.rpks.impl;

import ru.mai.lessons.rpks.IMoneyExchange;
import ru.mai.lessons.rpks.exception.ExchangeIsImpossibleException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;

public class MoneyExchange implements IMoneyExchange {
    public String exchange(Integer sum, String coinDenomination) throws ExchangeIsImpossibleException {
        if (coinDenomination.isEmpty()) {
            throw new ExchangeIsImpossibleException("Empty input string");
        }
        ArrayList<Integer> possibleCoinsForDenomination = new ArrayList<>(
                Arrays.stream(coinDenomination.split(", "))
                        .map(Integer::parseInt)
                        .filter(x -> x > 0)
                        .filter(x -> x <= sum)
                        .sorted(Comparator.reverseOrder())
                        .toList());
        if (possibleCoinsForDenomination.isEmpty()) {
            throw new ExchangeIsImpossibleException("Uncorrected input");
        }
        LinkedHashMap<Integer, Integer> exchanges = new LinkedHashMap<>();
        answerExchange(sum, 0, exchanges, possibleCoinsForDenomination);
        StringBuilder result = new StringBuilder();
        for (Integer key : exchanges.keySet()) {
            var value = exchanges.get(key);
            if (value != 0) {
                result.append(key).append("[").append(value).append("]").append(", ");
            }
        }
        result.setLength(result.length() - 2);
        return result.toString();
    }

    private void answerExchange(Integer sum, int index, LinkedHashMap<Integer, Integer> exchanges, ArrayList<Integer> money) {
        if (sum == 0) {
            return;
        }
        if (sum < money.get(index)) {
            answerExchange(sum, index + 1, exchanges, money);
        } else {
            if (exchanges.containsKey(money.get(index))) {
                exchanges.put(money.get(index), exchanges.get(money.get(index)) + 1);
            } else {
                exchanges.put(money.get(index), 1);
            }
            answerExchange(sum - money.get(index), index, exchanges, money);
        }
    }
}
