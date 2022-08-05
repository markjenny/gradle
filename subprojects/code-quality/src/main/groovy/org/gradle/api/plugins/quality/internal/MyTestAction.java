/*
 * Copyright 2022 the original author or authors.
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

package org.gradle.api.plugins.quality.internal;

import org.gradle.workers.WorkAction;
import org.gradle.workers.WorkParameters;

public abstract class MyTestAction implements WorkAction<WorkParameters.None> {
    @Override
    public void execute() {
        // Make the ClassLoader "dirty" by trying to load something from it
        Thread.currentThread().getContextClassLoader().getResource("abc");
    }
}
