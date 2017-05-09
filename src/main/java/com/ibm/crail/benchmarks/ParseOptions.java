/*
 * Crail SQL Benchmarks
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

public class ParseOptions {
    private Options options;
    private String banner;
    private String test;
    private String[] inputFiles;
    private String joinKey;
    private Action action;
    private boolean verbose;

    public ParseOptions(){

        this.banner = " _____  _____ _       ______                 _                          _    \n" +
                "/  ___||  _  | |      | ___ \\               | |                        | |   \n" +
                "\\ `--. | | | | |      | |_/ / ___ _ __   ___| |__  _ __ ___   __ _ _ __| | __\n" +
                " `--. \\| | | | |      | ___ \\/ _ \\ '_ \\ / __| '_ \\| '_ ` _ \\ / _` | '__| |/ /\n" +
                "/\\__/ /\\ \\/' / |____  | |_/ /  __/ | | | (__| | | | | | | | | (_| | |  |   < \n" +
                "\\____/  \\_/\\_\\_____/  \\____/ \\___|_| |_|\\___|_| |_|_| |_| |_|\\__,_|_|  |_|\\_\\\n" +
                "                                                                             \n" +
                "                                                                             ";
        options = new Options();
        options.addOption("h", "help", false, "show help.");
        options.addOption("t", "test", true, "which test to perform, options are (case insensitive): equiJoin, q65, readOnly ");
        options.addOption("i", "input", true, "comma separated list of input files/directories. " +
                "EquiJoin takes two files, q65 takes a tpc-ds data directory, and readOnly takes a file");
        options.addOption("k", "key", true, "key for EquiJoin, default is IntIndex");
        options.addOption("v", "verbose", false, "verbose");
        options.addOption("a", "action", true, "action to take. Your options are (important, no space between ','): \n" +
                " 1. count (default)\n" +
                " 2. collect,items[int, default: 100] \n" +
                " 3. save,filename[str, default: /tmp],format[str, default: parquet]  \n");

        // set defaults
        this.test = "readOnly";
        this.joinKey = "intKey";
        this.verbose = false;
        this.action = new Count();
    }

    public void show_help() {
        System.out.println(this.banner);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }

    private void errorAbort(String str) {
        System.err.println(str);
        show_help();
        System.exit(-1);
    }

    public void parse(String[] args) {
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;
        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
            if(cmd.hasOption("t")){
                this.test = cmd.getOptionValue("t").trim();
            }
            if(cmd.hasOption("v")){
                this.verbose = true;
            }
            if(cmd.hasOption("k")){
                this.joinKey = cmd.getOptionValue("k").trim();
            }
            if(cmd.hasOption("i")){
                // get the value and split it
                this.inputFiles = cmd.getOptionValue("i").split(",");
                for (String inputFile : this.inputFiles) {
                    inputFile.trim();
                }
            }
            if(cmd.hasOption("a")){
                String[] tokens = cmd.getOptionValue("a").split(",");
                if(tokens.length == 0) {
                    errorAbort("Failed to parse command line properties " + cmd.getOptionValue("a"));
                }
                if(tokens[0].compareToIgnoreCase("count") == 0){
                    this.action = new Count();
                } else if(tokens[0].compareToIgnoreCase("collect") == 0) {
                    int items = 0;
                    if(tokens.length != 2) {
                        items = 100;
                    } else {
                        items = Integer.parseInt(tokens[1].trim());
                    }
                    this.action = new Collect(items);
                } else if(tokens[0].compareToIgnoreCase("save") == 0) {
                    String fileName = (tokens.length >= 2) ? tokens[1].trim() : "/tmp";
                    String format = (tokens.length >= 3) ? tokens[2] : "parquet";
                    if(format.compareToIgnoreCase("Phi") == 0){
                        // FIXME: for now we have to expand it
                        format = "org.apache.spark.sql.execution.datasources.phi.PhiFileFormat";
                    }
                    this.action = new Save(format, fileName);
                } else {
                    errorAbort("ERROR: illegal action name : " + tokens[0]);
                }
            }

        } catch (ParseException e) {
            errorAbort("Failed to parse command line properties" + e);
        }
        // if not files are set
        if(this.inputFiles == null) {
            errorAbort("ERROR:" + " please specify some input files for the SQL test");
        }
        // check valid test names
        if(!isTestEquiJoin() && !isTestQ65() && !isTestReadOnly()) {
            errorAbort("ERROR: illegal test name : " + this.test);
        }
        /* some sanity checks */
        if(isTestEquiJoin() && this.inputFiles.length != 2){
            errorAbort("ERROR:" + this.test + " needs two files as inputs");
        }
    }

    public boolean isTestEquiJoin(){
        return this.test.compareToIgnoreCase("EquiJoin") == 0;
    }

    public boolean isTestQ65(){
        return this.test.compareToIgnoreCase("q65") == 0;
    }

    public boolean isTestReadOnly(){
        return this.test.compareToIgnoreCase("readOnly") == 0;
    }

    public String[] getInputFiles(){
        return this.inputFiles;
    }

    public String getJoinKey() {
        return this.joinKey;
    }

    public Action getAction(){
        return this.action;
    }

    public boolean getVerbose(){
        return this.verbose;
    }
}
