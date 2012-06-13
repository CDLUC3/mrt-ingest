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
 * enumeration for digest
 *
 * @author mreyes
 */

public enum DigestEnum {

	alder_32("Alder-32"),
	crc_32("CRC-32"),
	md2("MD2"),
	md5("MD5"),
	sha_1("SHA-1"),
	sha_256("SHA-256"),
	sha_384("SHA-384"),
	sha_512("SHA-512");

        protected String value = null;

	// constructor
	DigestEnum(String value) {
	   this.value = value;
	}

        /**
         * Return the digest type
         * @return digest type
         */
	public String getValue() {
	   return this.value;
	}

        /**
         * Match the digest to algorithm
         * @param value digest algorithm
         * @return enumerated Digest
         */
        public static DigestEnum setDigest(String value) {
            if (StringUtil.isEmpty(value)) return null;
            for (DigestEnum p : DigestEnum.values()) {
                if (p.getValue().equals(value)) {
                    return p;
                }
            }
            return null;
        }
}
