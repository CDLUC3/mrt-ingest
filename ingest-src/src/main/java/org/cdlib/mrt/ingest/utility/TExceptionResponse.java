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

import java.io.Serializable;  
  
import javax.xml.bind.annotation.XmlAccessType;  
import javax.xml.bind.annotation.XmlAnyElement;
import javax.xml.bind.annotation.XmlAccessorType;  
import javax.xml.bind.annotation.XmlAttribute;  
import javax.xml.bind.annotation.XmlElement;  
import javax.xml.bind.annotation.XmlNsForm;
import javax.xml.bind.annotation.XmlRootElement;
import javax.xml.bind.annotation.XmlSchema;
import javax.xml.bind.annotation.XmlSeeAlso;
import javax.xml.bind.annotation.XmlTransient;
import javax.xml.bind.annotation.XmlType;

@XmlRootElement
public class TExceptionResponse implements Serializable {
  
    private String location; 
    private String lineNumber; 
    private String className;
    private String methodName; 
    private String description; 
    private String error; 
    private String status;
    private String statusName;
    private String statusDescription;
    private String hTTPResponse;
    private String trace;

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getLocation() { return location; }
    public void setLocation(String location) { this.location = location; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getLineNumber() { return lineNumber; }
    public void setLineNumber(String lineNumber) { this.lineNumber = lineNumber; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getClassName() { return className; }
    public void setClassName(String className) { this.className = className; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getMethodName() { return methodName; }
    public void setMethodName(String methodName) { this.methodName = methodName; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getDescription() { return description; }
    public void setDescription(String description) { this.description = description; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getError() { return this.error; }
    public void setError(String error) { this.error = error; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getStatusName() { return statusName; }
    public void setStatusName(String statusName) { this.statusName = statusName; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getStatusDescription() { return statusDescription; }
    public void setStatusDescription(String statusDescription) { this.statusDescription = statusDescription; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getHTTPResponse() { return hTTPResponse; }
    public void setHTTPResponse(String hTTPResponse) { this.hTTPResponse = hTTPResponse; }

    @XmlElement(namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    public String getTrace() { return trace; }
    public void setTrace(String trace) { this.trace = trace; }


    @XmlRootElement(name="tException.EXTERNAL_SERVICE_UNAVAILABLE", namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    @XmlSeeAlso(TExceptionResponse.class)
    public static class EXTERNAL_SERVICE_UNAVAILABLE extends TExceptionResponse {}

    @XmlRootElement(name="tException.REQUEST_INVALID", namespace="http://uc3.cdlib.org/ontology/mrt/core/exc")
    @XmlSeeAlso(TExceptionResponse.class)
    public static class REQUEST_INVALID extends TExceptionResponse {}
}
