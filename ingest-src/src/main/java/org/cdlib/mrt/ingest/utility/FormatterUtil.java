/*
Copyright (c) 2011, Regents of the University of California
All rights reserved.

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions are
met:
 *
- Redistributions of source code must retain the above copyright notice,
  this list of conditions and the following disclaimer.
- Redistributions in binary form must reproduce the above copyright
  notice, this list of conditions and the following disclaimer in the
  documentation and/or other materials provided with the distribution.
- Neither the name of the University of California nor the names of its
  contributors may be used to endorse or promote products derived from
  this software without specific prior written permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO,
THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR
PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED
OF THE POSSIBILITY OF SUCH DAMAGE.
**********************************************************/
package org.cdlib.mrt.ingest.utility;


import java.io.File;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.util.Properties;

import org.cdlib.mrt.formatter.FormatterAbs;
import org.cdlib.mrt.formatter.FormatterInf;
import org.cdlib.mrt.formatter.FormatType;
import org.cdlib.mrt.utility.FileUtil;
import org.cdlib.mrt.utility.LoggerInf;
import org.cdlib.mrt.utility.StateInf;
import org.cdlib.mrt.utility.TException;
import org.cdlib.mrt.utility.TFileLogger;

/**
 * Simple formatting util
 * @author mreyes
 */
public class FormatterUtil
{

    private static final String NAME = "FormatterUtil";
    private static final String MESSAGE = NAME + ": ";
    private static final String EOL = "%0A";
    private static final String NL =  System.getProperty("line.separator");
    private static final boolean DEBUG = true;
    private static final String SUBJECT_TEMPLATE = "%s%s%s -- %s %s";		// Subject: service [instance]: status -- message: extra;

    private LoggerInf logger = null;
    private Properties conf = null;
    private Properties ingestProperties = null;

    public FormatterUtil() {
        logger = new TFileLogger("FormatterUtil", 10, 10);
    }
    
    /**
     * Run formatter on state
     * Originally seen in JerseyBase
     * @param state input state
     * @param format output format
     * @throws Exception
     */
    public String doStateFormatting(
            StateInf state,
            FormatType  format)
        throws Exception
    {
        try {
            PrintStream stream = null;
            FormatterInf formatter = getFormatter(format, logger);
            FormatterInf.Format formatterType = formatter.getFormatterType();
            String foundFormatType = formatterType.toString();
            FormatType formatType = format.valueOf(foundFormatType);
            String ext = formatType.getExtension();
            File file = FileUtil.getTempFile("state", "." + ext);
            FileOutputStream outStream = new FileOutputStream(file);
            stream = new PrintStream(outStream, true, "utf-8");
            formatter.format(state, stream);

	    return FileUtil.file2String(file);
	} catch (Exception e) {
	    // e.printStackTrace();
	    throw e;
	}
    }

    /**
     * Get StateInf formatter using Jersey FormatType
     * Involves mapping Jersey FormatType to FormatterInf.Format type
     * @param outputFormat  Jersey formattype
     * @param logger process logger
     * @return Formatter
     * @throws TException process exception
     */
    protected FormatterInf getFormatter(FormatType outputFormat, LoggerInf logger)
        throws TException
    {
        String formatS = null;
        try {
            formatS = outputFormat.toString();
            FormatterInf.Format formatterType = FormatterInf.Format.valueOf(formatS);
            return FormatterAbs.getFormatter(formatterType, logger);

        } catch (TException tex) {
            throw tex;

        } catch (Exception ex) {
            // if (DEBUG) System.err.println("getFormatter: stack:" + StringUtil.stackTrace(ex));
            throw new TException.REQUEST_ELEMENT_UNSUPPORTED("State formatter type not supported:" + formatS);
        }
    }


    public static String getSubject(String service, String status, String message) 
        throws TException
    {
        try {
            return getSubject(service, null, status, message, null);
        } catch (TException tex) {
            throw tex;
        }
    } 

    public static String getSubject(String service, String status, String message, String extra)
        throws TException
    {
        try {
           return getSubject(service, null, status, message, extra);
        } catch (TException tex) {
            throw tex;
        }
    } 

    // Subject: service [instance]: status -- message: extra
    public static String getSubject(String service, String instance, String status, String message, String... extras)
        throws TException
    {
	String extra = "";
        try {

	    if (instance != null) 
		instance = " [" + instance + "]: ";
	    else {
		instance = "";
		service += ": ";
		
	    }

	    if (message != null) 
		if (extras != null) message += ":";
	    if (extras != null) 
		for (String s : extras)
		    extra += s + ";";

            return String.format(SUBJECT_TEMPLATE, service, instance, status, message, extra);
        } catch (Exception ex) {
            throw new TException.GENERAL_EXCEPTION("Could not create subject line");
        }
    }

}
