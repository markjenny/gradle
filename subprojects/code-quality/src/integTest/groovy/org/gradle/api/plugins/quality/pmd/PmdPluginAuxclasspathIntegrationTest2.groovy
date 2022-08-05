/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.gradle.api.plugins.quality.pmd

import org.gradle.integtests.fixtures.AbstractIntegrationSpec

class PmdPluginAuxclasspathIntegrationTest2 extends AbstractIntegrationSpec {

    def "Reproduce the bug!"() {

        buildFile("""
            plugins {
                id 'java'
            }

            // Create a configuration which depends on our project outputs.
            configurations.register("bucket") {
                canBeConsumed = false
                canBeResolved = false
            }
            configurations.register("resolvable") {
                canBeConsumed = false
                canBeResolved = true
            }
            configurations.resolvable.extendsFrom(configurations.bucket)
            dependencies {
                bucket project
            }

            public abstract class MyTask extends DefaultTask {

                private FileCollection myClasspath;

                @TaskAction
                public void run() {
                    WorkQueue workQueue = project.services.get(WorkerExecutor).processIsolation(spec -> {
                        spec.getClasspath().from(getProject().getConfigurations().getByName("resolvable"));
                        spec.getForkOptions().getDebugOptions().getSuspend().set(true);
                        spec.getForkOptions().getDebugOptions().getEnabled().set(true);
                        spec.getForkOptions().getDebugOptions().getPort().set(5555);
                        spec.getForkOptions().jvmArgs("-Dcom.example.HEYHEYLOOKHERE=1")
                    });
                    workQueue.submit(org.gradle.api.plugins.quality.internal.MyTestAction, x -> {});
                }
            }
            tasks.register("doThing", MyTask) {
                dependsOn configurations.resolvable
            }
        """)

        expect:
        succeeds("doThing", "--info")
    }
}
