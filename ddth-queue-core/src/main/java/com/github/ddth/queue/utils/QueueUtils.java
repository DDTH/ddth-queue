package com.github.ddth.queue.utils;

import java.nio.charset.Charset;

import com.github.ddth.commons.utils.IdGenerator;

/**
 * Utilities and Constants
 * 
 * @author Thanh Nguyen <btnguyen2k@gmail.com>
 * 
 */
public class QueueUtils {
    public final static Charset UTF8 = Charset.forName("UTF-8");
    public final static IdGenerator IDGEN = IdGenerator.getInstance(IdGenerator.getMacAddr());
}
