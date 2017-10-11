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

import org.apache.commons.cli.Options;

/**
 * Created by atr on 11.10.17.
 */
public class FIOOptions extends TestOptions {
    private Options options;
    private String inputFiles;
    private String test;
    private String[][] fxOptions; // to parquet or spark
    private int parallelism;

    @Override
    public void parse(String[] args) {

    }

    @Override
    public void setWarmupConfig() {

    }

    @Override
    public void restoreInputConfig() {

    }

    @Override
    public boolean withWarmup() {
        return false;
    }


    public boolean isTestPaquetReading(){
        return this.test.compareToIgnoreCase("parquetreading") == 0;
    }

    public boolean isTestSFFReading(){
        return this.test.compareToIgnoreCase("sffreading") == 0;
    }

}
