package org.idf;

import org.idf.part1.CommonErrorsFinder;

import java.util.concurrent.ExecutionException;

public class Main {
    public static void main(String[] args) {
        CommonErrorsFinder cef=new CommonErrorsFinder("./logs.txt",3);
        try {
            System.out.println(cef.getTopCommonErrorsTypes());
        } catch (ExecutionException e) {
            throw new RuntimeException(e);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }
    }
}