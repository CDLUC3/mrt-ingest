Place custom docker images here which will not be re-used by other servivices.

Add a build block to the pom file
```
                            <name>itDockerRegistry/it-server-image</name>
                            <build>
                              <dockerFile>${project.basedir}/src/test/docker/it-server/Dockerfile</dockerFile>
                            </build>
```

Add the following to the goals section

```
                    <goal>build</goal>
```