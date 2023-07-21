package com.sdu.streaming.warehouse.functions;

import org.apache.flink.table.functions.ScalarFunction;

public class CurrencyExchange extends ScalarFunction {

    public double eval(Double sales, String currencyType) {
        switch (currencyType) {
            case "US":
                return sales * 6.89;
            case "EU":
                return sales * 7.85;
            default:
                return sales;
        }
    }

}
