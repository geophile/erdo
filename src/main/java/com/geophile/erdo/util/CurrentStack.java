package com.geophile.erdo.util;

import java.io.PrintWriter;
import java.io.StringWriter;

public class CurrentStack
{
    public static String asString()
    {
        StringWriter output = new StringWriter();
        new Exception().printStackTrace(new PrintWriter(output));
        return output.toString();
    }
}
