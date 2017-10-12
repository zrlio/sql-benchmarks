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

import org.apache.commons.cli.*;

/**
 * Created by atr on 11.10.17.
 */
public class FIOOptions extends TestOptions {
    private Options options;
    private String inputLocations;
    private String warmUpinputLocations;
    private boolean withWarmup;
    private String test;
    private String[][] fxOptions; // to parquet or spark
    private int parallel; // spark parallelization
    private long sizePerTask; // valid for writing
    private int align; // align to the block size
    private int requetSize;

    public FIOOptions(){
        options = new Options();
        this.inputLocations = null;
        this.warmUpinputLocations = null;
        this.test = "HdfsRead";
        this.parallel = 1;
        this.sizePerTask = 1024 * 1024; // 1MB
        this.align = 0; // alight to zero
        this.requetSize = 1024 * 1024; // 1MB
        this.withWarmup = false;

        options.addOption("h", "help", false, "show help.");
        options.addOption("i", "input", true, "[String] a location of input directory where files are read and written.");
        options.addOption("w", "warmupInput", true, "[String,...] a list of input files/directory used for warmup. Same semantics as the -i flag.");
        options.addOption("t", "test", true, "[String] which test to perform, HdfsRead, HdfsWrite, ParquetRead, ParquetWrite, SFFRead, SFFWrite. Default " + this.test);
        options.addOption("o", "options", true, "[<String,String>,...] options to set on SparkConf, NYI");
        options.addOption("p", "parallel", true, "[Int] number of parallel spark tasks");
        options.addOption("s", "size", true, "[Long] size per task. Takes prefixes like k, m, g, t");
        options.addOption("a", "align", true, "[Int] alignment");
        options.addOption("r", "requestSize", true, "[Int] request size ");
    }

    @Override
    public void parse(String[] args) {
        if (args != null) {
            CommandLineParser parser = new GnuParser();
            CommandLine cmd = null;
            try {
                cmd = parser.parse(options, args);

                if (cmd.hasOption("h")) {
                    show_help("SQL", this.options);
                    System.exit(0);
                }
                if (cmd.hasOption("i")) {
                    // get the value and split it
                    //this.inputLocations = cmd.getOptionValue("i").split(",");
                    this.inputLocations = cmd.getOptionValue("i");
                }
                if (cmd.hasOption("w")) {
                    // get the value and split it
                    //this.warmUpinputLocations = cmd.getOptionValue("w").split(",");
                    this.warmUpinputLocations = cmd.getOptionValue("w");
                    this.withWarmup = true;
                }
                if (cmd.hasOption("t")) {
                    this.test = cmd.getOptionValue("t").trim();
                }
                if (cmd.hasOption("o")) {
                    errorAbort(" -o is not yet implemented");
                }
                if(cmd.hasOption("p")){
                    this.parallel = Integer.parseInt(cmd.getOptionValue("p").trim());
                }
                if(cmd.hasOption("a")){
                    this.align = Integer.parseInt(cmd.getOptionValue("a").trim());
                }
                if(cmd.hasOption("r")){
                    this.requetSize = Integer.parseInt(cmd.getOptionValue("r").trim());
                }
                if(cmd.hasOption("s")){
                    this.sizePerTask = Utils.sizeStrToBytes2(cmd.getOptionValue("s").trim());
                }

            } catch (ParseException e) {
                errorAbort("Failed to parse command line properties" + e);
            }
        }
        if(!(isTestHdfsRead() || isTestHdfsWrite() || isTestPaquetRead() || isTestSFFRead())){
            errorAbort("Illegal test name for FIO : " + this.test);
        }
        if(this.inputLocations == null){
            errorAbort("Please specify input location with -i");
        }
    }

    @Override
    public void setWarmupConfig() {
        String temp = this.inputLocations;
        this.inputLocations = this.warmUpinputLocations;
        this.warmUpinputLocations = temp;
    }

    @Override
    public void restoreInputConfig() {
        String temp = this.inputLocations;
        this.inputLocations = this.warmUpinputLocations;
        this.warmUpinputLocations = temp;
    }

    @Override
    public boolean withWarmup() {
        return this.withWarmup;
    }


    public boolean isTestPaquetRead(){
        return this.test.compareToIgnoreCase("parquetread") == 0;
    }

    public boolean isTestSFFRead(){
        return this.test.compareToIgnoreCase("sffread") == 0;
    }

    public boolean isTestHdfsRead(){
        return this.test.compareToIgnoreCase("hdfsread") == 0;
    }

    public boolean isTestHdfsWrite(){
        return this.test.compareToIgnoreCase("hdfsWrite") == 0;
    }

    public String getInputLocations(){
        return this.inputLocations;
    }

    public int getParallel(){
        return this.parallel;
    }

    public long getSizePerTask(){
        return this.sizePerTask;
    }

    public int getAlign(){
        return this.align;
    }

    public int getRequetSize(){
        return this.requetSize;
    }

    public String getTestName(){
        return this.test;
    }
}
