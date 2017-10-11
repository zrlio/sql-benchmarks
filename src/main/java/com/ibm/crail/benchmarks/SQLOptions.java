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

import com.ibm.crail.benchmarks.sql.Action;
import com.ibm.crail.benchmarks.sql.Collect;
import com.ibm.crail.benchmarks.sql.Count;
import com.ibm.crail.benchmarks.sql.Save;
import org.apache.commons.cli.*;

import java.util.HashMap;
import java.util.Map;

/**
 * Created by atr on 11.10.17.
 *
 * This class is meant to encapsulate all SQL related options.
 */
public class SQLOptions extends TestOptions {
    private Options options;
    private String test;
    private String tpcdsQuery;
    private String[] inputFiles;
    private String[] warmupInputFiles;
    private boolean doWarmup;
    private String joinKey;
    private Action action;
    private String inputFormat;
    private String outputFormat;
    private Map<String, String> inputFormatOptions;
    private Map<String, String> outputFormatOptions;
    private boolean verbose;

    public SQLOptions(){
        options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", true, "which test to perform, options are (case insensitive): equiJoin, qXXX(tpcds queries), tpcds, readOnly ");
        options.addOption("i", "input", true, "comma separated list of input files/directories. " +
                "EquiJoin takes two files, TPCDS queries takes a tpc-ds data directory, and readOnly take a file or a directory with files");
        options.addOption("w", "warmupInput", true, "warmup files with the same semantics as the -i option");
        options.addOption("k", "key", true, "key for EquiJoin, default is IntIndex");
        options.addOption("v", "verbose", false, "verbose");
        options.addOption("a", "action", true, "action to take. Your options are (important, no space between ','): \n" +
                " 1. count (default)\n" +
                " 2. collect,items[int, default: 100] \n" +
                " 3. save,filename[str, default: /tmp]");
        options.addOption("if", "inputFormat", true, "input format (where-ever applicable) default: parquet");
        options.addOption("ifo", "inputFormatOptions", true, "input format options as key0,value0,key1,value1...");
        options.addOption("of", "outputFormat", true, "output format (where-ever applicable) default: parquet");
        options.addOption("ofo", "outputFormatOptions", true, "output format options as key0,value0,key1,value1...");

        // set defaults
        this.joinKey = "intKey";
        this.inputFormat = "parquet";
        this.outputFormat = "parquet";
        this.verbose = false;
        this.action = new Count();
        this.doWarmup = false;
        this.tpcdsQuery = null;

        this.inputFormatOptions = new HashMap<>(4);
        this.outputFormatOptions = new HashMap<>(4);
        /* at this point we set some defaults */
        // this is for parquet other options are "gzip", "snappy"
        this.outputFormatOptions.putIfAbsent("compression", "none");
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
                if (cmd.hasOption("t")) {
                    this.test = cmd.getOptionValue("t").trim();
                    if (this.test.charAt(0) == 'q') {
                    /* specific query test */
                        this.tpcdsQuery = this.test;
                    }
                }
                if (cmd.hasOption("v")) {
                    this.verbose = true;
                }
                if (cmd.hasOption("k")) {
                    this.joinKey = cmd.getOptionValue("k").trim();
                }
                if (cmd.hasOption("if")) {
                    this.inputFormat = cmd.getOptionValue("if").trim();
                    if (this.inputFormat.compareToIgnoreCase("nullio") == 0) {
                        this.inputFormat = "org.apache.spark.sql.NullFileFormat";
                    }
                }
                if (cmd.hasOption("of")) {
                    this.outputFormat = cmd.getOptionValue("of").trim();
                    if (this.outputFormat.compareToIgnoreCase("nullio") == 0) {
                        this.inputFormat = "org.apache.spark.sql.NullFileFormat";
                    }
                }
                if (cmd.hasOption("ofo")) {
                    String[] one = cmd.getOptionValue("ofo").trim().split(",");
                    if (one.length % 2 != 0) {
                        errorAbort("Illegal format for outputFormatOptions. Number of parameters " + one.length + " are not even");
                    }
                    for (int i = 0; i < one.length; i += 2) {
                        this.outputFormatOptions.put(one[i].trim(), one[i + 1].trim());
                    }
                }
                if (cmd.hasOption("ifo")) {
                    String[] one = cmd.getOptionValue("ifo").trim().split(",");
                    if (one.length % 2 != 0) {
                        errorAbort("Illegal format for inputFormatOptions. Number of parameters " + one.length + " are not even");
                    }
                    for (int i = 0; i < one.length; i += 2) {
                        this.inputFormatOptions.put(one[i].trim(), one[i + 1].trim());
                    }
                }

                if (cmd.hasOption("i")) {
                    // get the value and split it
                    this.inputFiles = cmd.getOptionValue("i").split(",");
                    for (String inputFile : this.inputFiles) {
                        inputFile.trim();
                    }
                }
                if (cmd.hasOption("w")) {
                    // get the value and split it
                    this.warmupInputFiles = cmd.getOptionValue("w").split(",");
                    for (String inputFile : this.warmupInputFiles) {
                        inputFile.trim();
                    }
                    this.doWarmup = true;
                }
                if (cmd.hasOption("a")) {
                    String[] tokens = cmd.getOptionValue("a").split(",");
                    if (tokens.length == 0) {
                        errorAbort("Failed to parse command line properties " + cmd.getOptionValue("a"));
                    }
                    if (tokens[0].compareToIgnoreCase("count") == 0) {
                        this.action = new Count();
                    } else if (tokens[0].compareToIgnoreCase("collect") == 0) {
                        int items = 0;
                        if (tokens.length != 2) {
                            items = 100;
                        } else {
                            items = Integer.parseInt(tokens[1].trim());
                        }
                        this.action = new Collect(items);
                    } else if (tokens[0].compareToIgnoreCase("save") == 0) {
                        String fileName = (tokens.length >= 2) ? tokens[1].trim() : "/sql-benchmark-output";
                        this.action = new Save(fileName);
                    } else {
                        errorAbort("ERROR: illegal action name : " + tokens[0]);
                    }
                }

            } catch (ParseException e) {
                errorAbort("Failed to parse command line properties" + e);
            }
            // if not files are set
            if (this.inputFiles == null) {
                errorAbort("ERROR:" + " please specify some input files for the SQL test");
            }
            // check valid test names
            if (!isTestEquiJoin() && !isTestQuery() && !isTestTPCDS() && !isTestReadOnly()) {
                errorAbort("ERROR: illegal test name : " + this.test);
            }
        /* some sanity checks */
            if (isTestEquiJoin() && this.inputFiles.length != 2) {
                errorAbort("ERROR:" + this.test + " needs two files as inputs");
            }
        }
    }

    @Override
    public void setWarmupConfig() {
     String[] temp = this.inputFiles;
     this.inputFiles = this.warmupInputFiles;
     this.warmupInputFiles = temp;
    }

    @Override
    public void restoreInputConfig() {
        String[] temp = this.inputFiles;
        this.inputFiles = this.warmupInputFiles;
        this.warmupInputFiles = temp;
    }

    @Override
    public boolean withWarmup() {
        return false;
    }

    public boolean isTestEquiJoin(){
        return this.test.compareToIgnoreCase("EquiJoin") == 0;
    }

    public boolean isTestQuery(){
        return !(this.tpcdsQuery == null);
    }

    public boolean isTestTPCDS(){
        return (this.test.compareToIgnoreCase("tpcds") == 0);
    }

    public boolean isTestReadOnly(){
        return this.test.compareToIgnoreCase("readOnly") == 0;
    }

    public String[] getInputFiles(){
        return this.inputFiles;
    }

    public void setInputFiles(String[] input){
        this.inputFiles = input;
    }

    public String[] getWarmupInputFiles(){
        return this.warmupInputFiles;
    }

    public boolean getDoWarmup(){
        return this.doWarmup;
    }

    public String getJoinKey() {
        return this.joinKey;
    }

    public Action getAction(){
        return this.action;
    }

    public void setAction(Action act){
        this.action = act;
    }

    public boolean getVerbose(){
        return this.verbose;
    }

    public String getInputFormat(){
        return this.inputFormat;
    }

    public String getOutputFormat(){
        return this.outputFormat;
    }

    public Map<String, String> getInputFormatOptions(){
        return this.inputFormatOptions;
    }

    public Map<String, String> getOutputFormatOptions(){
        return this.outputFormatOptions;
    }

    public String getTPCDSQuery(){
        return this.tpcdsQuery;
    }
}
