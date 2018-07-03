package com.jay.mrp;

import static org.junit.Assert.assertTrue;

import org.junit.Test;

import java.util.LinkedList;

/**
 * Unit test for simple App.
 */
public class AppTest 
{
    /**
     * Rigorous Test :-)
     */
    @Test
    public void shouldAnswerWithTrue()
    {
        LinkedList<String> alist = new LinkedList<String>();
        LinkedList<String> blist = new LinkedList<String>();
        alist.add("key1");
        blist.add("val1");

        for(String str1 : alist){
            for(String str2 : blist){
                System.out.print(str1 +" " + str2);

            }
        }

    }
}
