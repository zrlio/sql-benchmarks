/*
 * Spark Benchmarks
 *
 * Author: Animesh Trivedi <atr@zurich.ibm.com>
 *
 * Copyright (C) 2017, IBM Corporation
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */
package com.ibm.crail.benchmarks;

import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Options;

import java.io.Serializable;

/**
 * Created by atr on 10.10.17.
 *
 * This class is meant to just give a common point of reference to the test specific options.
 */
abstract public class TestOptions implements Serializable {
    public void show_help(String subsystem, Options opts) {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("com.ibm.crail.benchmarks.Main in " + subsystem, opts);
    }

    public void errorAbort(String str) {
        System.err.println("************ ERROR in " + this.getClass().getCanonicalName() + " *******************");
        System.err.println(str);
        System.err.println("**************************************");
        System.exit(-1);
    }

    abstract public void parse(String[] args);

    abstract public void setWarmupConfig();

    abstract public void restoreInputConfig();

    abstract public boolean withWarmup();
}
