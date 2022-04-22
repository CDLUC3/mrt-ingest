package org.cdlib.mrt.ingest;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Path;
import java.nio.file.Paths;

import org.cdlib.mrt.core.Identifier;
import org.cdlib.mrt.ingest.utility.ProfileUtil;
import org.cdlib.mrt.utility.TException;

public class IngestTestCore {
        public static final String RESOURCES = "src/test/resources/";
        public enum IngestProfile {
                merritt_test_content;

                private String path;

                IngestProfile(String path) {
                        this.path = path;
                }

                IngestProfile() {
                        this.path = this.name();
                }

                public Path getPath() {
                        return Paths.get(RESOURCES, "profile", path);
                }

                public File getFile() {
                        return getPath().toFile();
                }

                public Identifier getIdentifier() throws TException {
                        return new Identifier(this.name());
                }
        }

        public ProfileState getProfileState() throws TException, MalformedURLException {
                IngestProfile ip = IngestProfile.merritt_test_content;
                return ProfileUtil.getProfile(ip.getIdentifier(), ip.getFile());
        }
        
}
