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

import org.cdlib.mrt.utility.StringUtil;


/**
 * 
 * DataCite profile requires datacite.resourcetype (controlled vocabulary)
 * http://n2t.net/ezid/doc/apidoc.html#profile-datacite
 *
 * @author mreyes
 */


public enum ResourceTypeEnum {
	Collection,
	Dataset,
	Event,
	Film,
	Image,	
	InteractiveResource,
	Model,
	PhysicalObject,
	Service,
	Software,
	Sound,
	Text;

        public String getValue() {
           return this.name();
        }


        /**
         * Match the type to package
         * @param value package type
         * @return enum
         */
        public static ResourceTypeEnum setResourceType(String value) {
            if (StringUtil.isEmpty(value)) return null;
            for (ResourceTypeEnum p : ResourceTypeEnum.values()) {
                if (p.getValue().equalsIgnoreCase(value)) {
                    return p;
                }
            }
            return null;
        }
}
