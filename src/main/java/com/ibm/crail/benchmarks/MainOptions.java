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
public class MainOptions {
    private Options options;
    private String banner;
    private String subsystem;
    private boolean verbose;

    public MainOptions(){

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
        options.addOption("s", "subsystem", true, "which subsystem of tests to perform, options are: SQL, GraphX, FIO (case insensitive)");
        options.addOption("v", "verbose", false, "verbose");

        // set defaults
        this.subsystem = "SQL";
        this.verbose = false;
    }

    public void show_help() {
        System.out.println(this.banner);
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("Main", options);
    }

    private void errorAbort(String str) {
        show_help();
        System.err.println("************ ERROR in " + this.getClass().getCanonicalName() + "  *******************");
        System.err.println(str);
        System.err.println("**************************************");
        System.exit(-1);
    }

    private void showArgs(String[] args){
        System.out.println("*START*");
        for(int i = 0; i < args.length ; i++){
            System.out.println("\t index : " + i + " has " + args[i]);
        }
        System.out.println("*END*");
    }

    public String[] parseMainOptions(String[] allArgs) {

        String[] returnArgs = null;
        // zero based index
        int splitIndex = allArgs.length;
        for(int i = 0; i < allArgs.length ; i++){
            if(allArgs[i].trim().compareToIgnoreCase("--") == 0){
                splitIndex = i;
                break;
            }
        }
        String[] args = new String[splitIndex];
        for(int i = 0; i < args.length; i++){
            args[i] = allArgs[i];
        }
        /* we got some test specific arguments, just copy them out */
        if(splitIndex != allArgs.length){
            /* we had some return index */
            returnArgs = new String[allArgs.length - splitIndex - 1];
            for(int i = splitIndex + 1; i < allArgs.length; i++){
                returnArgs[i - splitIndex -1] = allArgs[i];
            }
        }

        /* we now parse args which are for Main */
        CommandLineParser parser = new GnuParser();
        CommandLine cmd = null;

        try {
            cmd = parser.parse(options, args);

            if (cmd.hasOption("h")) {
                show_help();
                System.exit(0);
            }
            if (cmd.hasOption("s")) {
                this.subsystem = cmd.getOptionValue("s").trim();
            }

        } catch (ParseException e) {
            errorAbort("Failed to parse command line properties" + e);
        }
        return returnArgs;
    }

    public String getSubsystem() {
        return this.subsystem;
    }
}